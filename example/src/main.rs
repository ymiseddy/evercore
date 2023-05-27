use std::result;
use std::sync::Arc;

use eventide::{Aggregate, EventStore, EventStoreError, EventContext};
use eventide::snapshot::Snapshot;
use eventide::event::Event;
use serde::{Serialize, Deserialize};

#[derive(thiserror::Error, Debug)]
enum AccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("Amount must be greater than 0")]
    InvalidAmount,

    #[error("No context")]
    NoContext,

    #[error("Event store error: {0}")]
    EventStoreError(#[from] EventStoreError),
}


#[derive(Serialize, Deserialize)]
struct AccountState {
    balance: u32,
}

struct Account<'a> {
    id: u64,
    version: u64,
    context: &'a EventContext<'a>,
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

impl<'a> Account<'_> {
    async fn new(ctx: &'a EventContext<'a>) -> Result<Account<'a>, AccountError> {
        Ok(Account {
            id: ctx.next_aggregate_id().await?,
            version: 0,
            context: ctx,
            state: AccountState { balance: 0 },
        })
    }

    pub fn deposit(&mut self, amount: u32) -> Result<(), AccountError> {
        if amount == 0 {
            return Err(AccountError::InvalidAmount);
        } 

        let data = DepositEvent { amount };

        self.context.publish(self, "deposit", &data)?;

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

        self.context.publish(self, "withdraw", &data)?;

        Ok(())
    }

}

impl<'a> Aggregate<'a> for Account<'a> {
    fn get_id(&self) -> u64 {
        self.id
    }


    fn get_type(&self) -> &str {
        "account"
    }

    fn get_version(&self) -> u64 {
        self.version
    }

    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError> {
        self.version = snapshot.version;
        let state: AccountState = snapshot.to_state()?;
        self.state = state;
        Ok(())
    }

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        self.version = event.version;
       
        let event_type = event.event_type.as_str();

        match event_type {
            "deposit" => {
                let data: DepositEvent = event.deserialize()?;
                self.state.balance += data.amount
            },
            "withdraw" => {
                let data: WithdrawEvent = event.deserialize()?;
                self.state.balance -= data.amount;
            },
            _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
        };
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

async fn deposit<'a>(ctx: &EventContext<'a>) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[tokio::main]
async fn main() {
    let memory = eventide::memory::MemoryStorageEngine::new();
    let event_store = EventStore::new(Arc::new(memory));
    let context = event_store.get_context();
    deposit(&context).await.unwrap();
}


/*

async fn deposit<'a>(ctx: &'a EventContext<'a>) -> Result<(), Box<dyn std::error::Error + 'a>> {
    Ok(())
}



#[tokio::main]
async fn main() {
    let memory = eventide::memory::MemoryStorageEngine::new();
    let event_store = EventStore::new(Arc::new(memory));

    let result = event_store.with_context(|ctx: &EventContext<'_>| deposit(ctx)).await;
}
*/
