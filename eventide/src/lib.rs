/// EventStore is a library for storing and retrieving events from an event store.
pub mod event;
pub mod memory;
pub mod snapshot;

use std::{sync::Arc,cell::RefCell};

use event::Event;
use serde::{de::DeserializeOwned, Serialize, Deserialize};
use snapshot::Snapshot;
use thiserror::Error;

/// Used for callbacks to ensure events are captured.
type CtxClosure = fn(&EventContext) -> Result<(), Box<dyn std::error::Error>>;

/// Used for callbacks to ensure events are captured - this allows a return value.
type CtxTypedClosure<T> = fn(&EventContext) -> Result<T, Box<dyn std::error::Error>>;


/// Aggregate is a trait that must be implemented by any aggregate that is to be stored in the event store.
pub trait Aggregate<'a> {
    fn get_id(&self) -> u64;
    fn set_id(&mut self, id: u64);
    fn get_type(&self) -> &str;
    fn get_version(&self) -> u64;
    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError>;
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;
    fn get_snapshot(&self) -> Result<Snapshot, EventStoreError>;
}

pub trait TypedAggregateImpl
{
    fn get_type(&self) -> &str;
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;
    /*
    fn request<TCommand, TEvent>(&self, request: TCommand) -> Result<(String, TEvent), EventStoreError> 
    where 
        TCommand: Serialize + DeserializeOwned,
        TEvent: Serialize + DeserializeOwned
    ;
    */
}

pub trait CanRequest<TCommand, TEvent>
where 
    TCommand: Serialize + DeserializeOwned,
    TEvent: Serialize + DeserializeOwned
{
    fn request(&self, request: TCommand) -> Result<(String, TEvent), EventStoreError>;
}



pub struct TypedAggregate<'a, T>
where 
    T: DeserializeOwned + Default + Serialize + TypedAggregateImpl
{
    id: u64,
    version: u64,
    context: Option<Arc<EventContext<'a>>>,
    state: T,
}

impl<'a, T> Aggregate<'a> for TypedAggregate<'a, T>
    where T: DeserializeOwned + Default + Serialize + TypedAggregateImpl + Clone
{

    fn get_id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    fn get_type(&self) -> &str {
        "none"
    }

    fn get_version(&self) -> u64 {
        self.version
    }

    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError> {
        self.version = snapshot.version;
        let state: T = snapshot.to_state()?;
        self.state = state;
        self.version = snapshot.version;
        Ok(())
    }

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        self.version = event.version;
        self.state.apply_event(event)?;
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

impl<'a, T> TypedAggregate<'a, T> 
    where 
        T: 'a +  DeserializeOwned + Default + Serialize + TypedAggregateImpl + Clone, 
        Self: Aggregate<'a>


{
    pub async fn new(ctx: Arc<EventContext<'a>>) -> Result<TypedAggregate<T>, EventStoreError> 
    {
        Ok(TypedAggregate {
            id: ctx.next_aggregate_id().await?,
            version: 0,
            context: Some(ctx),
            state: T::default(),
        })
    }

    pub fn reqeust<TCommand, TEvent>(&mut self, request: TCommand) -> Result<(), EventStoreError>
    where 
        TCommand: 'a + Serialize + DeserializeOwned,
        TEvent: 'a + Serialize + DeserializeOwned,
        T: CanRequest<TCommand, TEvent>
    {
        let ctx = match &self.context {
            Some(ctx) => ctx.clone(),
            None => return Err(EventStoreError::NoContext),
        };
        
        let (event_type, event) = CanRequest::<TCommand, TEvent>::request(&self.state, request)?;
        ctx.publish(self, &event_type, &event)?;

        Ok(())
    }

    pub async fn load(ctx: Arc<EventContext<'a>>, id: u64) -> Result<TypedAggregate<T>, EventStoreError>     {
        let mut state_aggregate = TypedAggregate{
            id,
            version: 0,
            context: Some(ctx.clone()),
            state: T::default(),
        };

        ctx.load(&mut state_aggregate).await?; 
        Ok(state_aggregate)
    }


    pub fn owned_state(&self) -> T {
        self.state.clone()
    }

}

#[async_trait::async_trait]
pub trait LoadableAggregate<'a>
where
    Self: Sized + Aggregate<'a> 
{
    fn new(context: Arc<EventContext<'a>>) -> Self;
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

    pub fn get_context(&self) -> Arc<EventContext> {
        Arc::new(EventContext::new(Arc::new(self)))
    }

}



pub type EventContextRc<'a> = Arc<EventContext<'a>>;
/// EventContext is a struct that is passed to the aggregate when an event is published.
pub struct EventContext<'a> {
    event_store: Arc<&'a EventStore>,
    captured_events: RefCell<Vec<Event>>,
}

impl<'a> EventContext<'a> {
    fn new(event_store: Arc<&'a EventStore>) -> EventContext {
        EventContext {
            event_store,
            captured_events: RefCell::new(Vec::new()),
        }
    }

    pub async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        self.event_store.next_aggregate_id().await
    }

    pub async fn load(&self, aggregate: &mut dyn Aggregate<'_>) -> Result<(), EventStoreError> {
        let snapshot = self.event_store.get_snapshot(aggregate.get_id(), aggregate.get_type()).await?;

        let snapshot_found = snapshot.is_some();
        if let Some(snapshot) = snapshot {
            aggregate.apply_snapshot(&snapshot)?;
        }

        let events = self
            .event_store
            .get_events(aggregate.get_id(), aggregate.get_type(), aggregate.get_version())
            .await?;

        if !snapshot_found && events.is_empty() {
            return Err(EventStoreError::AggregateNotFound((aggregate.get_type().to_string(), aggregate.get_id())));
        }

        for event in events {
            aggregate.apply_event(&event)?;
        }

        Ok(())
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
            source.get_version() + 1,
            event_type,
            data,
        )?;

        source.apply_event(&event)?;

        self.captured_events.borrow_mut().push(event);
        return Ok(());
    }

    pub async fn commit(&self) -> Result<(), EventStoreError> {
        let mut events = self.captured_events.borrow_mut();   
        self.event_store.save_events(&events).await?;
        events.clear();
        Ok(())
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
