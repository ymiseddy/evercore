use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use crate::event::Event;
use crate::snapshot::Snapshot;
use crate::EventStoreError;
use crate::EventContext;

/// Aggregate is a trait that must be implemented by any aggregate that is to be stored in the event store.
pub trait Aggregate<'a> {

    /// returns the id of the aggregate.
    fn id(&self) -> i64;

    /// sets the id of the aggregate.
    fn id_mut(&mut self, id: i64);

    /// returns frequency of snapshots for this aggregate. 0 means no snapshots.
    fn snapshot_frequency(&self) -> i32;

    /// returns the type of the aggregate.
    fn aggregate_type(&self) -> &str;

    /// returns the version of the aggregate.
    fn version(&self) -> i64;

    /// applies a snapshot to the aggregate.
    fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), EventStoreError>;

    /// applies an event to the aggregate.
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;

    /// returns a snapshot of the aggregate.
    fn take_snapshot(&self) -> Result<Snapshot, EventStoreError>;
}

/// A trait that must be implemented by any struct that is to be used as a StructBackedAggregate.
pub trait ComposedImpl
{
    fn get_type(&self) -> &str;
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;
    fn snapshot_frequency(&self) -> i32 {
        10
    }
}

/// A trait that must be implemented by any struct that is to be used as a StructBackedAggregate. 
/// It allows the aggregate do indicate the types of commands and events it accepts.
pub trait CanRequest<TCommand, TEvent>
where 
    TCommand: Serialize + DeserializeOwned,
    TEvent: Serialize + DeserializeOwned
{
    fn request(&self, request: TCommand) -> Result<(String, TEvent), EventStoreError>;
}


/// Generic implementation of an aggregate that is backed by a struct.
/// This saves having to implement the boilerplate code for each aggregate.
pub struct ComposedAggregate<T>
where 
    T: DeserializeOwned + Default + Serialize + ComposedImpl
{
    id: i64,
    version: i64,
    context: Option<Arc<EventContext>>,
    state: T,
}

impl<'a, T> Aggregate<'a> for ComposedAggregate<T>
    where T: DeserializeOwned + Default + Serialize + ComposedImpl + Clone
{

    fn id(&self) -> i64 {
        self.id
    }

    fn id_mut(&mut self, id: i64) {
        self.id = id;
    }

    fn aggregate_type(&self) -> &str {
        self.state.get_type()
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn snapshot_frequency(&self) -> i32 {
        self.state.snapshot_frequency()
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

    fn take_snapshot(&self) -> Result<Snapshot, EventStoreError> {
        let snapshot = Snapshot::new(
            self.id, 
            self.aggregate_type(), 
            self.version, 
            &self.state)?;

        Ok(snapshot)
    }

}

impl<'a, T> ComposedAggregate<T> 
    where 
        T: 'a +  DeserializeOwned + Default + Serialize + ComposedImpl + Clone, 
        Self: Aggregate<'a>


{
    pub async fn new(ctx: Arc<EventContext>, natural_key: Option<&str>) -> Result<ComposedAggregate<T>, EventStoreError> 
    {
        let state = T::default();
        let aggregate_type = state.get_type();


        Ok(ComposedAggregate {
            id: ctx.next_aggregate_id(aggregate_type, natural_key).await?,
            version: 0,
            context: Some(ctx),
            state
        })
    }

    pub fn request<TCommand, TEvent>(&mut self, request: TCommand) -> Result<(), EventStoreError>
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

    pub async fn load(ctx: Arc<EventContext>, id: i64) -> Result<ComposedAggregate<T>, EventStoreError>     {
        let mut state_aggregate = ComposedAggregate{
            id,
            version: 0,
            context: Some(ctx.clone()),
            state: T::default(),
        };

        ctx.load(&mut state_aggregate).await?; 
        Ok(state_aggregate)
    }

    pub fn state(&self) -> &T {
        &self.state
    }


    pub fn owned_state(&self) -> T {
        self.state.clone()
    }
}
