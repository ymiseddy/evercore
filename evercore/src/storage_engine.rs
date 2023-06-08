use crate::{snapshot::Snapshot, EventStoreError, event::Event};


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


