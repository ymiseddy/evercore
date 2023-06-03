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
}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl EventStoreStorageEngine for MemoryStorageEngine {

    async fn next_aggregate_id(&self, _aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        memory_store.id += 1;
        let id = memory_store.id;

        if natural_key.is_some() {
            memory_store.natural_key_map.insert(natural_key.unwrap().to_string(), id);
        }

        Ok(id)
    }

    async fn get_events(
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

    async fn get_snapshot(
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
