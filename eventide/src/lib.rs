/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod memory;
pub mod snapshot;
pub mod aggregate;
pub mod contexts;

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
    async fn save_events(&self, events: &[Event]) -> Result<(), EventStoreError>;
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), EventStoreError>;
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

    pub async fn save_events(&self, events: &[Event]) -> Result<(), EventStoreError> {
        self.storage_engine.save_events(events).await
    }

    pub async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), EventStoreError> {
        self.storage_engine.save_snapshot(snapshot).await
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
    ApplySnapshotError(Box<dyn std::error::Error>),

    #[error("Error applying event.")]
    ApplyEventError(Box<dyn std::error::Error>),

    #[error("Error during context callback.")]
    ContextError(Box<dyn std::error::Error>),

    #[error("Attempt to publish an event before context is set.")]
    NoContext,
    
    #[error("Unexpected error during context callback.")]
    ContextError2(String),
}

#[cfg(test)]
mod tests {
}
