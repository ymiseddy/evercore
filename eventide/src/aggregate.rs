use serde::Serialize;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use crate::event::Event;
use crate::snapshot::Snapshot;
use crate::EventStoreError;
use crate::EventContext;

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

/// StructAggregateImpl is a trait that must be implemented by any struct that is to be used as a StructBackedAggregate.
pub trait StructAggregateImpl
{
    fn get_type(&self) -> &str;
    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError>;
}

/// CanRequest is a trait that must be implemented by any struct that is to be used as a StructBackedAggregate. 
/// It allows the aggregate do indicate the types of commands and events it accepts.
pub trait CanRequest<TCommand, TEvent>
where 
    TCommand: Serialize + DeserializeOwned,
    TEvent: Serialize + DeserializeOwned
{
    fn request(&self, request: TCommand) -> Result<(String, TEvent), EventStoreError>;
}


/// StructBackedAggregate is a generic implementation of an aggregate that is backed by a struct.
/// This saves having to implement the boilerplate code for each aggregate.
pub struct StructBackedAggregate<'a, T>
where 
    T: DeserializeOwned + Default + Serialize + StructAggregateImpl
{
    id: u64,
    version: u64,
    context: Option<Arc<EventContext<'a>>>,
    state: T,
}

impl<'a, T> Aggregate<'a> for StructBackedAggregate<'a, T>
    where T: DeserializeOwned + Default + Serialize + StructAggregateImpl + Clone
{

    fn get_id(&self) -> u64 {
        self.id
    }

    fn set_id(&mut self, id: u64) {
        self.id = id;
    }

    fn get_type(&self) -> &str {
        self.state.get_type()
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

impl<'a, T> StructBackedAggregate<'a, T> 
    where 
        T: 'a +  DeserializeOwned + Default + Serialize + StructAggregateImpl + Clone, 
        Self: Aggregate<'a>


{
    pub async fn new(ctx: Arc<EventContext<'a>>) -> Result<StructBackedAggregate<T>, EventStoreError> 
    {
        Ok(StructBackedAggregate {
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

    pub async fn load(ctx: Arc<EventContext<'a>>, id: u64) -> Result<StructBackedAggregate<T>, EventStoreError>     {
        let mut state_aggregate = StructBackedAggregate{
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


