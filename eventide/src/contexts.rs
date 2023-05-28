use std::{sync::Arc, cell::RefCell};
use serde::de::DeserializeOwned;
use crate::{EventStore, event::Event, EventStoreError, aggregate::Aggregate};


/// EventContext is a struct that is passed to the aggregate when an event is published.
pub struct EventContext {
    event_store: Arc<EventStore>,
    captured_events: RefCell<Vec<Event>>,
}

impl EventContext {
    pub fn new(event_store: Arc<EventStore>) -> EventContext {
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
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), EventStoreError> {
        let events = self.captured_events.borrow().clone();   
        self.event_store.save_events(&events).await?;
        self.captured_events.borrow_mut().clear();
        Ok(())
    }

}
