/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod memory;
pub mod snapshot;

use std::{sync::Arc,cell::RefCell};

use event::Event;
use serde::de::DeserializeOwned;
use snapshot::Snapshot;
use thiserror::Error;

/// Used for callbacks to ensure events are captured.
type CtxClosure = fn(&EventContext) -> Result<(), Box<dyn std::error::Error>>;

/// Used for callbacks to ensure events are captured - this allows a return value.
type CtxTypedClosure<T> = fn(&EventContext) -> Result<T, Box<dyn std::error::Error>>;


/// Aggregate is a trait that must be implemented by any aggregate that is to be stored in the event store.
pub trait Aggregate<'a> {
    fn get_id(&self) -> u64;
    fn get_type(&self) -> &str;
    fn get_version(&self) -> u64;
    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError>;
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;
    fn get_snapshot(&self) -> Result<Snapshot, EventStoreError>;
}


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
    async fn save_events(&self, events: &Vec<Event>) -> Result<(), EventStoreError>;
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), EventStoreError>;
}


/// EventStore is the main struct for the event store.
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

    pub async fn save_events(&self, events: &Vec<Event>) -> Result<(), EventStoreError> {
        self.storage_engine.save_events(events).await
    }

    pub async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), EventStoreError> {
        self.storage_engine.save_snapshot(snapshot).await
    }

    pub async fn with_context_returns<T, Fut>(&self, ctx_fn: impl FnOnce(&EventContext) -> Fut) -> Result<T, EventStoreError>
    where
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error>>>
     {
        let ctx = EventContext::new(self);
        let result = ctx_fn(&ctx).await.map_err(EventStoreError::ContextError);
        let events = &ctx.captured_events.borrow();   
        self.save_events(events).await?;
        return result;
    }

    pub fn get_context<'a>(&'a self) -> EventContext<'a> {
        EventContext::new(self)
    }

    /*
    pub async fn with_context<'a, Fut>(&self, ctx_fn: impl FnOnce(&EventContext) -> Fut) -> Result<(), EventStoreError>
    where
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>> + a
     {
        let ctx = EventContext::new(self);
        {
            ctx_fn(&ctx).await.map_err(EventStoreError::ContextError)?;
        }
        let events = &ctx.captured_events.borrow();   
        self.save_events(events).await
    }
        */

}

/// EventContext is a struct that is passed to the aggregate when an event is published.
pub struct EventContext<'a> {
    event_store: &'a EventStore,
    captured_events: RefCell<Vec<Event>>,
}

impl EventContext<'_>  {
    fn new<'a>(event_store: &'a EventStore) -> EventContext {
        EventContext {
            event_store,
            captured_events: RefCell::new(Vec::new()),
        }
    }

    pub async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        self.event_store.next_aggregate_id().await
    }

    pub fn publish<T>(
        &self,
        source: &mut dyn Aggregate,
        event_type: &str,
        data: &T,
    ) -> Result<(), EventStoreError>
    where
        T: serde::Serialize + DeserializeOwned
    {
        let event = Event::new(
            source.get_id(),
            source.get_type(),
            source.get_version(),
            event_type,
            data,
        )?;

        source.apply_event(&event)?;

        self.captured_events.borrow_mut().push(event);
        return Ok(());
    }

}

/// EventStoreError is the error type for the event store.
#[derive(Error, Debug)]
pub enum EventStoreError {
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
    
    #[error("Unexpected error during context callback.")]
    ContextError2(String),
}

#[cfg(test)]
mod tests {
}
