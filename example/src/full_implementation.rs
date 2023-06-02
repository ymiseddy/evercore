use std::sync::Arc;

use eventide::{EventStoreError, contexts::EventContext, aggregate::Aggregate, snapshot::Snapshot, event::Event, EventStore};
use serde::{Deserialize, Serialize};



#[derive(thiserror::Error, Debug)]
enum AccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("Amount must be greater than 0")]
    InvalidAmount,

    #[error("Event store error: {0}")]
    EventStoreError(#[from] EventStoreError),
}


#[derive(Serialize, Deserialize, Debug, Default)]
struct AccountState {
    balance: u32,
}

struct Account {
    id: u64,
    version: u64,
    context: Option<Arc<EventContext>>,
    state: AccountState,
}

#[derive(Serialize, Deserialize)] 
struct WithdrawEvent { 
    amount: u32,
}

#[derive(Serialize, Deserialize)]
struct DepositEvent {
    amount: u32,
}

impl<'a> Account {
    async fn new(ctx: Arc<EventContext>) -> Result<Account, AccountError> {
        Ok(Account {
            id: ctx.next_aggregate_id().await?,
            version: 0,
            context: Some(ctx),
            state: AccountState { balance: 0 },
        })
    }

    async fn load(ctx: Arc<EventContext>, id: u64) -> Result<Account, AccountError>     {
        let mut account = Account{
            id,
            version: 0,
            context: Some(ctx.clone()),
            state: AccountState { balance: 0 },
        };

        ctx.load(&mut account).await?; 
        Ok(account)
    }


    pub fn get_balance(&self) -> u32 {
        self.state.balance
    }

    pub fn deposit(&mut self, amount: u32) -> Result<(), AccountError> {
        if amount == 0 {
            return Err(AccountError::InvalidAmount);
        } 

        let data = DepositEvent { amount };

        let context = self.context.clone().unwrap();
        context.publish(self, "deposit", &data)?;

        Ok(())
    } 

    pub fn withdraw(&mut self, amount: u32) -> Result<(), AccountError> {
        if amount == 0 {
            return Err(AccountError::InvalidAmount);
        } 

        if self.state.balance < amount {
            return Err(AccountError::InsufficientFunds);
        }


        let data = WithdrawEvent { amount };
        let context = self.context.clone().unwrap();
        context.publish(self, "withdraw", &data)?;

        Ok(())
    }

}

impl<'a> Aggregate<'a> for Account {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    fn get_type(&self) -> &str {
        "account"
    }

    fn snapshot_frequency(&self) -> u32 {
        10 
    } 

    fn get_version(&self) -> u64 {
        self.version
    }

    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError> {
        self.version = snapshot.version;
        let state: AccountState = snapshot.to_state()?;
        println!("State: {:?}", state);
        self.state = state;
        self.version = snapshot.version;
        Ok(())
    }

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        self.version = event.version;
       
        let event_type = event.event_type.as_str();
        println!("Applying event type: {}", event_type);

        match event_type {
            "deposit" => {
                let data: DepositEvent = event.deserialize()?;
                self.state.balance += data.amount
            },
            "withdraw" => {
                let data: WithdrawEvent = event.deserialize()?;
                self.state.balance -= data.amount;
            },
            _ => return Err(EventStoreError::ApplyEventError("Unknown event type".to_string()))
        };
        self.version = event.version;
        println!("After event applied State: {:?}", self.state);
        Ok(())
    }

    fn get_snapshot(&self) -> Result<Snapshot, EventStoreError> {
        let snapshot = Snapshot::new(
            self.id, 
            self.get_type(), 
            self.version, 
            &self.state)?;

        Ok(snapshot)
    }
}

pub(crate) async fn account_example(event_store: Arc<EventStore>) {
    let id: u64;

    let context = event_store.clone().get_context();
    {
        let mut account = Account::new(context.clone()).await.unwrap();
        id = account.get_id();
        account.deposit(400).unwrap();
        account.withdraw(300).unwrap();
        let balance = account.get_balance();
        println!("Account Id {} Balance: {}",id, balance);
    }


    context.commit().await.unwrap();
    drop(context);

    let context = event_store.get_context();
    {
        println!("Loading account");
        let account = Account::load(context.clone(), id).await.unwrap();
        //account.deposit(400).unwrap();
        //account.withdraw(300).unwrap();
        let balance = account.get_balance();
        println!("Balance: {}", balance);
    }
}
