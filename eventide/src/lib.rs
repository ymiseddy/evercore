/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod snapshot;
pub mod aggregate;
pub mod contexts;

#[cfg(feature = "memory")]
pub mod memory;

use crate::contexts::EventContext;

use std::sync::Arc;

use event::Event;
use snapshot::Snapshot;
use thiserror::Error;


/// EventStorageEnging is a trait that must be implemented by any storage engine that is to be used by the event store.
#[async_trait::async_trait]
pub trait EventStoreStorageEngine {
    async fn next_aggregate_id(&self) -> Result<u64, EventStoreError>;

    async fn get_events(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
        version: u64,
    ) -> Result<Vec<Event>, EventStoreError>;
    async fn get_snapshot(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError>;
    async fn write_updates(&self, events: &[Event], snapshot: &[Snapshot]) -> Result<(), EventStoreError>;
}

/// EventStore is the main struct for the event store.
#[derive(Clone)]
pub struct EventStore {
    storage_engine: Arc<dyn EventStoreStorageEngine>,
}


impl EventStore {

    /// Create a new EventStore with the given storage engine.
    pub fn new(storage_engine: Arc<dyn EventStoreStorageEngine>) -> EventStore {
        EventStore { storage_engine }
    }

    pub async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        self.storage_engine.next_aggregate_id().await 
    }

    pub async fn get_events(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
        version: u64,
    ) -> Result<Vec<Event>, EventStoreError> {
        self.storage_engine.get_events(aggregate_id, aggregate_type, version).await
    }

    pub async fn get_snapshot(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        self.storage_engine.get_snapshot(aggregate_id, aggregate_type).await
    }

    pub async fn write_updates(&self, events: &[Event], snapshots: &[Snapshot]) -> Result<(), EventStoreError> {
        self.storage_engine.write_updates(events, snapshots).await?;
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
    AggregateNotFound((String, u64)),

    #[error("Error serializaing event.")]
    EventSerializationError(serde_json::Error),

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

    #[error("Attempt to publish an event before context is set.")]
    NoContext,

    #[error("Error in storage engine.")]
    StorageEngineError(Box<dyn std::error::Error>),
    
    #[error("Error in storage engine.")]
    StorageEngineConnectionError(String),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Serialize, Deserialize};

    use crate::{aggregate::{StructAggregateImpl, CanRequest, StructBackedAggregate}, EventStoreError};


    #[derive(Default, Clone, Serialize, Deserialize)]
    struct Account {
        user_id: u64,
        balance: u64,
    }
    
    #[derive(Serialize, Deserialize)]
    struct AccountCreation {
        user_id: u64,
    }

    #[derive(Serialize, Deserialize)]
    struct AccountUpdate {
        amount: u64,
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

    impl StructAggregateImpl for Account {
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
            let mut account = StructBackedAggregate::<Account>::new(context.clone()).await.unwrap();

            account.request(AccountCommands::CreateAccount(AccountCreation { user_id: 1 })).unwrap();

            account.request(AccountCommands::CreditAccount(AccountUpdate { amount: 100 })).unwrap();

            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 50 })).unwrap();

            account.request(AccountCommands::DebitAccount(AccountUpdate { amount: 10 })).unwrap();

            let state = account.get_state();
            assert!(state.balance == 40);
        }
        context.commit().await.unwrap();
    }



}
