/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod snapshot;
pub mod aggregate;
pub mod contexts;

#[cfg(feature = "memory")]
pub mod memory;

use crate::contexts::EventContext;

use std::{sync::{Arc, PoisonError}, future::Future};

use event::Event;
use snapshot::Snapshot;
use thiserror::Error;


/// EventStorageEnging is a trait that must be implemented by any storage engine that is to be used by the event store.
#[async_trait::async_trait]
pub trait EventStoreStorageEngine {
    async fn create_aggregate_instance(&self, aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError>;
    async fn get_aggregate_instance_id(&self, aggregate_type: &str, natural_key: &str) -> Result<Option<i64>, EventStoreError>;

    async fn read_events(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
        version: i64,
    ) -> Result<Vec<Event>, EventStoreError>;

    async fn read_snapshot(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError>;
    async fn write_updates(&self, events: &[Event], snapshot: &[Snapshot]) -> Result<(), EventStoreError>;
}

/// EventStore is the main struct for the event store.
#[derive(Clone)]
pub struct EventStore {
    storage_engine: Arc<dyn EventStoreStorageEngine + Send + Sync>,
}


impl EventStore {

    /// Create a new EventStore with the given storage engine.
    pub fn new(storage_engine: Arc<dyn EventStoreStorageEngine + Send + Sync>) -> EventStore {
        EventStore { storage_engine }
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
/*
async fn example<Fut>(f: impl FnOnce(i32, i32) -> Fut)
where
    Fut: Future<Output = bool>,
{
    f(1, 2).await;
}
    */
    pub async fn with_context<Fut>(self: Arc<EventStore>, context_task: impl FnOnce(Arc<EventContext>) -> Fut ) 
       -> Result<(), EventStoreError> 
    where 
        Fut: Future<Output = Result<(), EventStoreError>> + Send + 'static
        
    {
        let context = self.get_context();
        context_task(context.clone()).await?;
        context.commit().await?;
        Ok(())
    }

    pub fn get_context(self: Arc<EventStore>) -> Arc<EventContext> {
        Arc::new(EventContext::new(self))
    }
}

/// EventStoreError is the error type for the event store.
#[derive(Error, Debug)]
pub enum EventStoreError {

    #[error("Aggregate not found: {0:?}")]
    AggregateNotFound((String, i64)),

    #[error("Error serializaing event.")]
    EventSerializationError(serde_json::Error),
    
    #[error("Error serializaing metadata for event.")]
    EventMetaDataSerializationError(serde_json::Error),

    #[error("Error deserializaing event.")]
    EventDeserializationError(serde_json::Error),

    #[error("Error serializaing snapshot.")]
    SnapshotSerializationError(serde_json::Error),

    #[error("Error deserializaing snapshot.")]
    SnapshotDeserializationError(serde_json::Error),

    #[error("Error saving events.")]
    SaveEventsError(Box<dyn std::error::Error>),

    #[error("Error saving snapshot.")]
    SaveSnapshotError(Box<dyn std::error::Error>),

    #[error("Error getting events.")]
    GetEventsError(Box<dyn std::error::Error>),

    #[error("Error getting snapshot.")]
    GetSnapshotError(Box<dyn std::error::Error>),

    #[error("Error getting next aggregate id.")]
    GetNextAggregateIdError(Box<dyn std::error::Error>),

    #[error("Error applying snapshot.")]
    ApplySnapshotError(String),

    #[error("Error processing request.")]
    RequestProcessingError(String),

    #[error("Error applying event.")]
    ApplyEventError(String),

    #[error("Error during context callback.")]
    ContextError(Box<dyn std::error::Error>),

    /*
    #[error("Error acquiring lock in context.")]
    ContextErrorLock(#[from] PoisonError),
    */
    #[error("Error acquiring lock in context.")]
    ContextPoisonError,
    
    #[error("Error in storage engine.")]
    ContextErrorOther(String),

    #[error("Attempt to publish an event before context is set.")]
    NoContext,

    #[error("Error in storage engine.")]
    StorageEngineError(Box<dyn std::error::Error>),
   
    #[error("Error in storage engine.")]
    StorageEngineErrorOther(String),
    
    #[error("Error in storage engine.")]
    StorageEngineConnectionError(String),

    #[error("Aggregate instance not found.")]
    AggregateInstanceNotFound,

}


impl<T> From<PoisonError<T>> for EventStoreError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::ContextPoisonError
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashMap};
    use serde::{Serialize, Deserialize};
    use crate::{aggregate::{ComposedImpl, CanRequest, ComposedAggregate}, EventStoreError, EventStoreStorageEngine};


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

    impl ComposedImpl for Account {
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
        let event_store = Arc::new(event_store);
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
        let event_store = Arc::new(event_store);
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
        let event_store = Arc::new(event_store);
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
        let event_store = Arc::new(event_store);
        let context = event_store.clone().get_context();
        context.add_metadata("user", "chavez");
        context.add_metadata("ip_address", "10.100.1.100");
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
