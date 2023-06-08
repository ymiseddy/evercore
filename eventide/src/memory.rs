use std::{sync::{Arc, Mutex}, collections::HashMap};

use crate::{ EventStoreError, event::Event, snapshot::Snapshot, EventStoreStorageEngine};


type DynMemoryStore = Arc<Mutex<MemoryStore>>;

#[derive(Default)]
pub struct MemoryStore {
    id: i64, 
    events: Vec<Event>,
    snapshots: Vec<Snapshot>,
    natural_key_map: HashMap<String, i64>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            id: 0,
            events: Vec::new(),
            snapshots: Vec::new(),
            natural_key_map: HashMap::new(),
        }
    }
}

/// Memory based storage engine for EventStore
///
/// This is a simple in-memory storage engine for EventStore. It is not intended for production use.
/// It is useful for testing and as a reference implementation.
///
pub struct MemoryStorageEngine {
    memory_store: DynMemoryStore,
}

impl MemoryStorageEngine {
    pub fn new() -> MemoryStorageEngine {
        MemoryStorageEngine {
            memory_store: Arc::new(Mutex::new(MemoryStore::new())), 
        }
    }

    pub fn snapshot_count(&self) -> usize {
        let memory_store = self.memory_store.lock().unwrap();
        memory_store.snapshots.len()
    }

    pub fn snapshot_count_by_aggregate_type(&self, aggregate_type: &str) -> usize {
        let memory_store = self.memory_store.lock().unwrap();
        let mut count = 0;
        for snapshot in &memory_store.snapshots {
            if snapshot.aggregate_type == aggregate_type {
                count += 1;
            }
        }
        count
    }

}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl EventStoreStorageEngine for MemoryStorageEngine {

    async fn create_aggregate_instance(&self, _aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        memory_store.id += 1;
        let id = memory_store.id;

        if natural_key.is_some() {
            memory_store.natural_key_map.insert(natural_key.unwrap().to_string(), id);
        }

        Ok(id)
    }

    async fn get_aggregate_instance_id(&self, _aggregate_type: &str, natural_key: &str) -> Result<Option<i64>, EventStoreError> {
        let memory_store = self.memory_store.lock().unwrap();
        let id = memory_store.natural_key_map.get(natural_key);
        match id {
            Some(id) => Ok(Some(*id)),
            None => Ok(None)
        }
    }

    async fn read_events(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
        version: i64,
    ) -> Result<Vec<Event>, EventStoreError> {
        let memory_store = self.memory_store.lock().unwrap();
        let mut events = Vec::new();

        for event in &memory_store.events {
            if event.aggregate_id == aggregate_id && event.aggregate_type == aggregate_type && event.version > version {
                events.push(event.clone());
            }
        }
        Ok(events)
    }

    async fn read_snapshot(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        let memory_store = self.memory_store.lock().unwrap();
        let iter = memory_store.snapshots.iter().rev();
        for snapshot in iter {
            if snapshot.aggregate_id == aggregate_id && snapshot.aggregate_type == aggregate_type {
                return Ok(Some(snapshot.clone()));
            }
        }
        Ok(None)
    }

    async fn write_updates(&self, events: &[Event], snapshots: &[Snapshot]) -> Result<(), EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        for event in events {
            memory_store.events.push(event.clone());
        }
        for snapshot in snapshots {
            memory_store.snapshots.push(snapshot.clone());
        }
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use serde::{Serialize, Deserialize};

    use super::*;

    #[derive(Serialize, Deserialize, Debug)]
    struct UserCreate {
        name: String,
        email: String,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Context {
        user_id: i32,
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct UserState {
        name: String,
        email: String,
    }


    #[tokio::test]
    async fn ensure_create_aggregate() {
        let storage_engine = MemoryStorageEngine::new();
        let aggregate_type = "test";
        let natural_key = "test";
        let id = storage_engine.create_aggregate_instance(aggregate_type, Some(natural_key)).await.unwrap();
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn ensure_can_get_aggregate_instance_id() {
        let storage_engine = MemoryStorageEngine::new();
        let aggregate_type = "test";
        let natural_key = "test";
        storage_engine.create_aggregate_instance(aggregate_type, Some(natural_key)).await.unwrap();

        let id = storage_engine.get_aggregate_instance_id(aggregate_type, natural_key).await.unwrap().unwrap();
        assert_eq!(id, 1);
    }

    #[tokio::test]
    async fn ensure_can_write_events() {
        let event_data = UserCreate {
            name: "test".to_string(),
            email: "rtest@example.com".to_string(),
        };

        let event = Event::new(1, "test", 1, "created", &event_data).unwrap();
        
        let state = UserState {
            name: "test".to_string(),
            email: "rtest@example.com".to_string(),
        };
        let snapshot = Snapshot::new(1, "test", 1, &state).unwrap();

        let storage_engine = MemoryStorageEngine::new();
        storage_engine.write_updates(&[event.clone()], &[snapshot.clone()]).await.unwrap();

        let events = storage_engine.read_events(1, "test", 0).await.unwrap();
        let retrieved_snapshot = storage_engine.read_snapshot(1, "test").await.unwrap().unwrap();

        assert_eq!(events[0].data, event.data);
        assert_eq!(events[0].aggregate_id, 1);
        assert_eq!(events[0].event_type, "created");
        assert_eq!(events[0].version, 1);

        assert_eq!(retrieved_snapshot.data, snapshot.data);
        assert_eq!(retrieved_snapshot.aggregate_id, 1);
        assert_eq!(retrieved_snapshot.aggregate_type, "test");
        assert_eq!(retrieved_snapshot.version, 1);


    }
    
    #[tokio::test]
    async fn ensure_missing_aggregate_instance_retrieval_returns_none() {
        let storage_engine = MemoryStorageEngine::new();
        let result = storage_engine.get_aggregate_instance_id("test", "test").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ensure_missing_snapshot_returns_none() {
        let storage_engine = MemoryStorageEngine::default();
        let retrieved_snapshot = storage_engine.read_snapshot(1, "test").await.unwrap();
        assert!(retrieved_snapshot.is_none());
    }

}
