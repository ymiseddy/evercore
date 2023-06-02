use std::sync::Mutex;
use thiserror::Error;
use chrono::{DateTime, Utc};

use eventide::{EventStoreError, event::Event, snapshot::Snapshot, EventStoreStorageEngine};

struct SqliteStorageEngine {
    conn: Mutex<rusqlite::Connection>,
}

#[derive(Error, Debug)]
enum SqliteStorageEngineError {
    #[error("Storage engine error")]
    StorageEngineError,

    #[error("Sqlite error")]
    SqliteError(#[from] rusqlite::Error),

    #[error("Sqlite numeric code error.")]
    SqliteNumericCodeError(usize),
}


impl From<usize> for SqliteStorageEngineError {
    fn from(err: usize) -> Self {
        Self::SqliteNumericCodeError(err)
    }
}

impl SqliteStorageEngine {
    pub fn new(conn: rusqlite::Connection) -> Self {
        Self { conn: Mutex::new(conn) }
    }

    pub async fn table_init(&self) -> Result<(), EventStoreError> {
        let lock = self.conn.lock();
        let conn = match lock {
            Ok(conn) => conn,
            Err(_) => return Err(EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::StorageEngineError))),
        };

        // Create aggregate id table

        conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregate_ids (
                id INTEGER PRIMARY KEY,
            )", 
            ()
        ).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;


        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                aggregate_id INTEGER NOT NULL,
                aggregate_type TEXT NOT NULL,
                version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                event_data TEXT NOT NULL,
                created_at TEXT NOT NULL
            )", 
            ()
        ).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                aggregate_id INTEGER NOT NULL,
                aggregate_type TEXT NOT NULL,
                version INTEGER NOT NULL,
                snapshot_data TEXT NOT NULL,
                created_at TEXT NOT NULL
            )",
            (),
        ).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        return Ok(());

    }
}

#[async_trait::async_trait]
impl EventStoreStorageEngine for SqliteStorageEngine {

    async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        let lock = self.conn.lock();
        let conn = match lock {
            Ok(conn) => conn,
            Err(_) => return Err(EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::StorageEngineError))),
        };

        let mut stmt = conn.prepare("INSERT INTO aggregate_ids (id) VALUES (NULL)").map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
        stmt.execute(()).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let id = conn.last_insert_rowid() as u64;

        return Ok(id);
    }

    async fn get_events(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
        version: u64,
    ) -> Result<Vec<Event>, EventStoreError> {

        // Retrieve events for aggregate type and version
        let lock = self.conn.lock();
        let conn = match lock {
            Ok(conn) => conn,
            Err(_) => return Err(EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::StorageEngineError))),
        };

        let mut stmt = conn.prepare("SELECT * FROM events WHERE aggregate_id = ? AND aggregate_type = ? AND version >= ? ORDER BY version ASC").map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let mut rows = stmt.query([aggregate_id.to_string(), aggregate_type.to_string(), version.to_string()]).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let mut events = Vec::new();

        while let Some(row) = rows.next().map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))? {
            let version: u64 = row.get(3).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
            let event_type: String = row.get(4).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
            let event_data: String = row.get(5).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
            let created_at: String = row.get(6).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

            // let event = Event::new(aggregate_id, event_type, event_data, created_at);
            let event = Event {
                    aggregate_id, 
                    aggregate_type: aggregate_type.to_string(), 
                    version, 
                    event_type,
                    data: event_data};

            events.push(event);
        }
return Ok(events);
    }

    async fn get_snapshot(
        &self,
        aggregate_id: u64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {

        let lock = self.conn.lock();
        let conn = match lock {
            Ok(conn) => conn,
            Err(_) => return Err(EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::StorageEngineError))),
        };

        let mut stmt = conn.prepare("SELECT * FROM snapshots WHERE aggregate_id = ? AND aggregate_type = ? ORDER BY version DESC LIMIT 1").map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let mut rows = stmt.query([aggregate_id.to_string(), aggregate_type.to_string()]).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let row = match rows.next().map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))? {
            Some(row) => row,
            None => return Ok(None),
        };

        let version: u64 = row.get(3).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        let snapshot_data: String = row.get(4).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;


        let snapshot = Snapshot {
            aggregate_id, 
            aggregate_type: aggregate_type.to_string(), 
            version, 
            data: snapshot_data
        };

        return Ok(Some(snapshot));

        
    }

    async fn write_updates(&self, events: &[Event], snapshots: &[Snapshot]) -> Result<(), EventStoreError> {

        let lock = self.conn.lock();
        let mut conn = match lock {
            Ok(conn) => conn,
            Err(_) => return Err(EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::StorageEngineError))),
        };

        let created_at = Utc::now();
        
        // Convert to ISO 8601 format
        let created_at = created_at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();



        let tx = conn.transaction().map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        for event in events {
            let mut stmt = tx.prepare("INSERT INTO events (aggregate_id, aggregate_type, version, event_type, event_data, created_at) VALUES (?, ?, ?, ?, ?, ?)").map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
            stmt.execute([event.aggregate_id.to_string(), event.aggregate_type.to_string(), event.version.to_string(), event.event_type.to_string(), event.data.to_string(), created_at.to_string()]).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
        }

        for snapshot in snapshots {
            let mut stmt = tx.prepare("INSERT INTO snapshots (aggregate_id, aggregate_type, version, snapshot_data) VALUES (?, ?, ?, ?)").map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
            stmt.execute([snapshot.aggregate_id.to_string(), snapshot.aggregate_type.to_string(), snapshot.version.to_string(), snapshot.data.to_string()]).map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;
        }

        tx.commit().map_err(|err| EventStoreError::StorageEngineError(Box::new(SqliteStorageEngineError::SqliteError(err))))?;

        return Ok(());
    }



}
