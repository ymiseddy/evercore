/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod snapshot;
pub mod aggregate;
pub mod contexts;
mod error;
mod storage_engine;


pub use error::EventStoreError;
pub use storage_engine::EventStoreStorageEngine;

#[cfg(feature = "memory")]
pub mod memory;

use crate::contexts::EventContext;

use std::{sync::Arc, future::Future};

use event::Event;
use snapshot::Snapshot;


/// EventStore is the main struct for the event store.
#[derive(Clone)]
pub struct EventStore {
    storage_engine: Arc<dyn EventStoreStorageEngine + Send + Sync>,
}

pub type SharedEventStore = Arc<EventStore>;
pub type SharedEventContext = Arc<EventContext>;

impl EventStore {

    /// Create a new EventStore with the given storage engine.
    pub fn new(storage_engine: Arc<dyn EventStoreStorageEngine + Send + Sync>) -> SharedEventStore {
        Into::into(EventStore { storage_engine })
    }

    pub async fn next_aggregate_id(&self, aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError> {
        self.storage_engine.create_aggregate_instance(aggregate_type, natural_key).await 
    }

    pub async fn get_events(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
        version: i64,
    ) -> Result<Vec<Event>, EventStoreError> {
        self.storage_engine.read_events(aggregate_id, aggregate_type, version).await
    }

    pub async fn get_snapshot(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        self.storage_engine.read_snapshot(aggregate_id, aggregate_type).await
    }

    pub async fn write_updates(&self, events: &[Event], snapshots: &[Snapshot]) -> Result<(), EventStoreError> {
        self.storage_engine.write_updates(events, snapshots).await?;
        Ok(())
    }
    

    /// Execute a task within a contest, returning a result.
    pub async fn with_context_returning<Fut, T>(self: SharedEventStore, context_task: impl FnOnce(SharedEventContext) -> Fut ) 
       -> Result<T, EventStoreError> 
    where 
        Fut: Future<Output = Result<T, EventStoreError>> + Send + 'static
        
    {
        let context = self.get_context();
        let result = context_task(context.clone()).await?;
        context.commit().await?;
        Ok(result)
    }

    /// Execute a task within a contest.
    pub async fn with_context<Fut>(self: SharedEventStore, context_task: impl FnOnce(SharedEventContext) -> Fut ) 
       -> Result<(), EventStoreError> 
    where 
        Fut: Future<Output = Result<(), EventStoreError>> + Send + 'static
        
    {
        let context = self.get_context();
        context_task(context.clone()).await?;
        context.commit().await?;
        Ok(())
    }

    pub fn get_context(self: SharedEventStore) -> SharedEventContext {
        Arc::new(EventContext::new(self))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashMap};
    use serde::{Serialize, Deserialize};
    use crate::{aggregate::{Composable, CanRequest, ComposedAggregate}, EventStoreError, EventStoreStorageEngine};


    #[derive(Default, Clone, Serialize, Deserialize)]
    struct Account {
        user_id: i64,
        balance: i64,
    }
    
    #[derive(Serialize, Deserialize)]
    struct AccountCreation {
        user_id: i64,
    }

    #[derive(Serialize, Deserialize)]
    struct AccountUpdate {
        amount: i64,
    }

    #[derive(Serialize, Deserialize)]
    enum AccountCommands {
        CreateAccount(AccountCreation),
        CreditAccount(AccountUpdate),
        DebitAccount(AccountUpdate),
    }


    #[derive(Serialize, Deserialize)]
    enum AccountEvents {
        AccountCreated(AccountCreation),
        AccountCredited(AccountUpdate),
        AccountDebite(AccountUpdate),
    }

    impl Composable for Account {
        fn get_type(&self) -> &str {
            "account"
        }

        fn apply_event(&mut self, event: &crate::event::Event) -> Result<(), crate::EventStoreError> {

            let event = event.deserialize::<AccountEvents>()?;

            match event {
                AccountEvents::AccountCreated(event) => {
                    self.user_id = event.user_id;
                },
                AccountEvents::AccountCredited(event) => {
                    self.balance += event.amount;
                },
                AccountEvents::AccountDebite(event) => {
                    if event.amount > self.balance {
                        return Err(EventStoreError::RequestProcessingError("Insufficient funds".to_string()));
                    }
                    self.balance -= event.amount;
                },
            }
            return Ok(());
        }
    }


    impl CanRequest<AccountCommands, AccountEvents> for Account {
        fn request(&self, request: AccountCommands) -> Result<(String, AccountEvents), crate::EventStoreError> {

            match request {
                AccountCommands::CreateAccount(command) => {
                    Ok(("created".to_string(), AccountEvents::AccountCreated(command)))
                },
                AccountCommands::CreditAccount(command) => {
                    if command.amount > self.balance {
                    }
                    Ok(("credited".to_string(), AccountEvents::AccountCredited(command)))
                },
                AccountCommands::DebitAccount(command) => {
                    Ok(("debited".to_string(), AccountEvents::AccountDebite(command)))
                },
            }
        }
    }

    #[tokio::test]
    async fn test_eventstore() {
        let memory = crate::memory::MemoryStorageEngine::new();
        let event_store = crate::EventStore::new(Arc::new(memory));
        let context = event_store.get_context();
        {
            let mut account = ComposedAggregate::<Account>::new(context.clone(), None).await.unwrap();

            account.request(AccountCommands::CreateAccount(AccountCreation { user_id: 1 })).unwrap();
            account.request(AccountCommands::CreditAccount(AccountUpdate { amount: 100 })).unwrap();
            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 50 })).unwrap();
            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 10 })).unwrap();

            let state = account.state();
            assert!(state.balance == 40);
        }
        context.commit().await.unwrap();
    }

    #[tokio::test]
    async fn ensure_events_mutate_state() {
        let memory = crate::memory::MemoryStorageEngine::new();
        let event_store = crate::EventStore::new(Arc::new(memory));
        let context = event_store.clone().get_context();
        {
            let mut account = ComposedAggregate::<Account>::new(context.clone(), None).await.unwrap();
            account.request(AccountCommands::CreateAccount(AccountCreation { user_id: 1 })).unwrap();
            account.request(AccountCommands::CreditAccount(AccountUpdate { amount: 100 })).unwrap();
            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 50 })).unwrap();
            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 10 })).unwrap();

            let state = account.state();
            assert!(state.balance == 40);
        }
        context.commit().await.unwrap();

        let context = event_store.get_context();
        {
            let account = ComposedAggregate::<Account>::load(context, 1).await.unwrap();
            let state = account.state();
            assert!(state.balance == 40);
        }
    }

    #[tokio::test]
    async fn ensure_takes_snapshots() {
        let memory = crate::memory::MemoryStorageEngine::new();
        let memory = Arc::new(memory);
        let event_store = crate::EventStore::new(memory.clone());
        let context = event_store.clone().get_context();
        {
            let mut account = ComposedAggregate::<Account>::new(context.clone(), None).await.unwrap();
            account.request(AccountCommands::CreateAccount(AccountCreation { user_id: 1 })).unwrap();
            for (_i, _) in (0..100).enumerate() {
                account.request(AccountCommands::CreditAccount(AccountUpdate { amount: 100 })).unwrap();
            }

            let state = account.state();
            assert!(state.balance == 100*100);
        }
        context.commit().await.unwrap();
        let context = event_store.get_context();
        {
            let account = ComposedAggregate::<Account>::load(context, 1).await.unwrap();
            let state = account.state();
            assert!(state.balance == 100*100);
        }
        assert_eq!(memory.snapshot_count(), 10);
    }
    
    #[tokio::test]
    async fn ensure_captures_metadata() {
        let memory = crate::memory::MemoryStorageEngine::new();
        let memory = Arc::new(memory);
        let event_store = crate::EventStore::new(memory.clone());
        let context = event_store.clone().get_context();
        context.add_metadata("user", "chavez").unwrap();
        context.add_metadata("ip_address", "10.100.1.100").unwrap();
        {
            let mut account = ComposedAggregate::<Account>::new(context.clone(), Some("chavez_account")).await.unwrap();
            account.request(AccountCommands::CreateAccount(AccountCreation { user_id: 1 })).unwrap();
        }
        context.commit().await.unwrap();

        let id = memory.get_aggregate_instance_id("account", "chavez_account").await.unwrap().unwrap();

        let events = memory.read_events(id, "account", 0).await.unwrap();
        let event = events[0].clone();
        let hashmap: HashMap<String, String> = event.deserialize_metadata().unwrap().unwrap();

        assert_eq!(hashmap.get("user").unwrap(), "chavez");
        assert_eq!(hashmap.get("ip_address").unwrap(), "10.100.1.100");
    }
}
