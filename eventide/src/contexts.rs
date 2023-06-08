use std::{sync::Arc, cell::RefCell, collections::HashMap};
use serde::de::DeserializeOwned;
use crate::{EventStore, event::Event, EventStoreError, aggregate::Aggregate, snapshot::Snapshot};


/// A struct that is passed to the aggregate when it is loaded or created.
pub struct EventContext {
    event_store: Arc<EventStore>,
    captured_snapshots: RefCell<Vec<Snapshot>>,
    captured_events: RefCell<Vec<Event>>,
    metadata: RefCell<HashMap<String, String>>
}

impl EventContext {
    pub fn new(event_store: Arc<EventStore>) -> EventContext {
        EventContext {
            event_store,
            captured_snapshots: RefCell::new(Vec::new()),
            captured_events: RefCell::new(Vec::new()),
            metadata: RefCell::new(HashMap::new())
        }
    }

    pub fn add_metadata(&self, key: &str, value: &str) -> () {
        self.metadata.borrow_mut().insert(key.to_string(), value.to_string());
        ();
    }

    pub async fn next_aggregate_id(&self, aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError> {
        self.event_store.next_aggregate_id(aggregate_type, natural_key).await
    }

    pub async fn load(&self, aggregate: &mut dyn Aggregate<'_>) -> Result<(), EventStoreError> {
        let snapshot = self.event_store.get_snapshot(aggregate.id(), aggregate.aggregate_type()).await?;

        let snapshot_found = snapshot.is_some();
        if let Some(snapshot) = snapshot {
            aggregate.apply_snapshot(&snapshot)?;
        }

        let events = self
            .event_store
            .get_events(aggregate.id(), aggregate.aggregate_type(), aggregate.version())
            .await?;

        if !snapshot_found && events.is_empty() {
            return Err(EventStoreError::AggregateNotFound((aggregate.aggregate_type().to_string(), aggregate.id())));
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
        let new_version = source.version() + 1;

        let mut event = Event::new(
            source.id(),
            source.aggregate_type(),
            new_version,
            event_type,
            data,
        )?;

        if !self.metadata.borrow().is_empty() {
            event.add_metadata(&self.metadata)?;
        }

        let snapshot_frequency: i64 = source.snapshot_frequency().into();
        if snapshot_frequency > 0 && new_version % snapshot_frequency == 0 {
            let snapshot = source.take_snapshot()?;
            self.captured_snapshots.borrow_mut().push(snapshot);
        }

        source.apply_event(&event)?;

        self.captured_events.borrow_mut().push(event);
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), EventStoreError> {
        let events = self.captured_events.borrow().clone();   
        let snapshots = self.captured_snapshots.borrow().clone();
        self.event_store.write_updates(&events, &snapshots).await?;
        Ok(())
    }

}
