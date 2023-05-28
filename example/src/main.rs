use std::result;
use std::sync::Arc;

use eventide::{LoadableAggregate, Aggregate, EventStore, EventStoreError, EventContext, TypedAggregateImpl, TypedAggregate, CanRequest};
use eventide::snapshot::Snapshot;
use eventide::event::Event;
use serde::{Serialize, Deserialize};

#[derive(thiserror::Error, Debug)]
enum AccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("Amount must be greater than 0")]
    InvalidAmount,

    #[error("Event store error: {0}")]
    EventStoreError(#[from] EventStoreError),
}


#[derive(Serialize, Deserialize, Debug)]
struct AccountState {
    balance: u32,
}

impl Default for AccountState {
    fn default() -> Self {
        AccountState { balance: 0 }
    }
}

struct Account<'a> {
    id: u64,
    version: u64,
    context: Option<Arc<EventContext<'a>>>,
    state: AccountState,
}

#[derive(Serialize, Deserialize)] struct WithdrawEvent { amount: u32,
}

#[derive(Serialize, Deserialize)]
struct DepositEvent {
    amount: u32,
}

impl<'a> Account<'_> {
    async fn new(ctx: Arc<EventContext<'a>>) -> Result<Account<'a>, AccountError> {
        Ok(Account {
            id: ctx.next_aggregate_id().await?,
            version: 0,
            context: Some(ctx),
            state: AccountState { balance: 0 },
        })
    }

    async fn load(ctx: Arc<EventContext<'a>>, id: u64) -> Result<Account<'a>, AccountError>     {
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

impl<'a> Aggregate<'a> for Account<'a> {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
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
            _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct User {
    name: String,
    email: String,
    password_hash: String,
}


#[derive(Serialize, Deserialize, Debug)]
enum UserCommands {
    UserCreated {
        name: String,
        email: String,
        password_hash: String,
    },
    UserUpdated {
        name: String,
        email: String,
    },
    UserPasswordUpdated {
        password_hash: String,
    },
}

impl TypedAggregateImpl for User {

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        let event_type = event.event_type.as_str();
        println!("Applying event type: {}", event_type);
        let data: UserCommands = event.deserialize()?;

        match data {
            UserCommands::UserCreated { name, email, password_hash } => {
                self.name = name;
                self.email = email;
                self.password_hash = password_hash;
            },
            UserCommands::UserUpdated { name, email } => {
                self.name = name;
                self.email = email;
            },
            UserCommands::UserPasswordUpdated { password_hash } => {
                self.password_hash = password_hash;
            },
        };
      
      /*

        match event_type {
            "user_created" => {
                let data: UserCommands = event.deserialize()?;
                match data {
                    UserCommands::UserCreated { name, email, password_hash } => {
                        self.name = name;
                        self.email = email;
                        self.password_hash = password_hash;
                    },
                    _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
                };
            },
            "user_updated" => {
                let data: UserCommands = event.deserialize()?;
                match data {
                    UserCommands::UserUpdated { name, email } => {
                        self.name = name;
                        self.email = email;
                    },
                    _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
                };
            },
            "password_updated" => {
                let data: UserCommands = event.deserialize()?;
                match data {
                    UserCommands::UserPasswordUpdated { password_hash } => {
                        self.password_hash = password_hash;
                    },
                    _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
                };
            },
            _ => return Err(EventStoreError::ContextError2("Unknown event type".to_string()))
        };
        */
        
        Ok(())
    }

    fn get_type(&self) -> &str {
        "user"
    }

}

impl CanRequest<UserCommands, UserCommands> for User {
    fn request(&self, request: UserCommands) -> Result<(String, UserCommands), EventStoreError> 
    {
        match request {
            UserCommands::UserCreated { name, email, password_hash } => {
                let event = UserCommands::UserCreated { name, email, password_hash };
                Ok(("user_created".to_string(), event))
            },
            UserCommands::UserUpdated { name, email } => {
                let event = UserCommands::UserUpdated { name, email };
                Ok(("user_updated".to_string(), event))
            },
            UserCommands::UserPasswordUpdated { password_hash } => {
                let event = UserCommands::UserPasswordUpdated { password_hash };
                Ok(("password_updated".to_string(), event))
            },
        }

    }
}


async fn account_example() {
    let memory = eventide::memory::MemoryStorageEngine::new();
    let event_store = EventStore::new(Arc::new(memory));

    let id: u64;

    let context = event_store.get_context();
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

#[tokio::main]
async fn main() {
    account_example().await;
    
    let memory = eventide::memory::MemoryStorageEngine::new();
    let event_store = EventStore::new(Arc::new(memory));
    let context = event_store.get_context();
 

    let mut user = TypedAggregate::<User>::new(context.clone()).await.unwrap();
    let id = user.get_id();
    println!("User Id: {}", id);
    user.reqeust(UserCommands::UserCreated {
        name: "John Doe".to_string(),
        email: "jdoe@example.com".to_string(),
        password_hash: "123456".to_string(),
    }).unwrap();
    context.commit().await.unwrap();


    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);

    let context = event_store.get_context();
    let mut user = TypedAggregate::<User>::load( context.clone(), id).await.unwrap();
    user.reqeust(UserCommands::UserUpdated {
        name: "Samuel Jackson".to_string(),
        email: "sammyj@example.com".to_string(),
    }).unwrap();
    context.commit().await.unwrap();
    
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);


    let context = event_store.get_context();
    let user = TypedAggregate::<User>::load( context.clone(), id).await.unwrap();
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);

    let create_command = UserCommands::UserCreated {
        name: "John Doe".to_string(),
        email: "jdoe@example.com".to_string(),
        password_hash: "123456".to_string(),
    };


    serde_json::to_writer_pretty(std::io::stdout(), &create_command).unwrap();
   
}
