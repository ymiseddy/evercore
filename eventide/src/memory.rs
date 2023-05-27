use std::sync::{Arc, Mutex};

use crate::{ EventStoreError, event::Event, snapshot::Snapshot, EventStoreStorageEngine};


type DynMemoryStore = Arc<Mutex<MemoryStore>>;

pub struct MemoryStore {
    id: u64, 
    events: Vec<Event>,
    snapshots: Vec<Snapshot>,
}

impl MemoryStore {
    pub fn new() -> MemoryStore {
        MemoryStore {
            id: 0,
            events: Vec::new(),
            snapshots: Vec::new(),
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

#[async_trait::async_trait]
impl EventStoreStorageEngine for MemoryStorageEngine {

    async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        memory_store.id += 1;
        Ok(memory_store.id)
    }

    async fn get_events(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
        version: u64,
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
        aggregate_id: u64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        let memory_store = self.memory_store.lock().unwrap();
        for snapshot in &memory_store.snapshots {
            if snapshot.aggregate_id == aggregate_id && snapshot.aggregate_type == aggregate_type {
                return Ok(Some(snapshot.clone()));
            }
        }
        Ok(None)
    }

    async fn save_events(&self, events: &Vec<Event>) -> Result<(), EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        for event in events {
            memory_store.events.push(event.clone());
        }
        Ok(())
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), EventStoreError> {
        let mut memory_store = self.memory_store.lock().unwrap();
        memory_store.snapshots.push(snapshot);
        Ok(())
    }
}
