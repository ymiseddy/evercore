#[forbid(unsafe_code)]

use std::{collections::HashMap, sync::Arc};
use futures::lock::Mutex;
use eventide::{event::Event, snapshot::Snapshot, EventStoreError, EventStoreStorageEngine};
use sqlx::{AnyPool, pool::PoolConnection, Row};

pub enum DbType {
    Sqlite,
    Postgres,
    // Mysql,
}

struct PostgresqlBuilder;

trait QueryBuilder {
    fn build_queries(&self) -> Vec<String>;
    fn insert_or_get_aggregate_type(&self) -> String;
    fn insert_or_get_event_type(&self) -> String;
    fn insert_aggregate_instance(&self) -> String;
    fn insert_event(&self) -> String;
    fn insert_snapshot(&self) -> String;
    fn get_events(&self) -> String;
    fn get_snapshot(&self) -> String;
}


impl QueryBuilder for PostgresqlBuilder {

   fn build_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();

        let q = "CREATE TABLE IF NOT EXISTS aggregate_types (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            UNIQUE(name)
        );";
        queries.push(q.to_string());
        
        let q = "CREATE TABLE IF NOT EXISTS event_types (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            UNIQUE(name)
        );";
        queries.push(q.to_string());

        let q = "CREATE TABLE IF NOT EXISTS aggregate_instances (
            id BIGSERIAL PRIMARY KEY,
            aggregate_type_id BIGINT NOT NULL,
            natural_key VARCHAR(255),
            UNIQUE(aggregate_type_id, natural_key),
            CONSTRAINT fk_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        );";
        queries.push(q.to_string());

        let q = "CREATE TABLE IF NOT EXISTS events (
            id BIGSERIAL PRIMARY KEY,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            event_type_id BIGINT NOT NULL,
            data JSONB NOT NULL,
            metadata JSONB,
            UNIQUE(aggregate_id, version),
            CONSTRAINT fk_aggregate_id
                FOREIGN KEY(aggregate_id)
                    REFERENCES aggregate_instances(id),
            CONSTRAINT fk_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id),
            CONSTRAINT fk_event_type_id
                FOREIGN KEY(event_type_id)
                    REFERENCES event_types(id)
        );";
        queries.push(q.to_string());


        let q = "CREATE TABLE IF NOT EXISTS snapshots (
            id BIGSERIAL PRIMARY KEY,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            data JSONB NOT NULL,
            UNIQUE(aggregate_id, version),
            CONSTRAINT fk_aggregate_id
                FOREIGN KEY(aggregate_id)
                    REFERENCES aggregate_instances(id),
            CONSTRAINT fk_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        );";
        queries.push(q.to_string());
        queries
    }

    fn insert_or_get_aggregate_type(&self) -> String {
        // Select or insert the aggregate type.
        "INSERT INTO aggregate_types (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id;"
        .to_string()
    }

    fn insert_or_get_event_type(&self) -> String {
        // Select or insert the event type.
        "INSERT INTO event_types (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id;"
        .to_string()
    }

    fn insert_aggregate_instance(&self) -> String {
        "INSERT INTO aggregate_instances (id, aggregate_type_id, natural_key) VALUES ($1, $2, $3)"
        .to_string()
    }

    fn insert_event(&self) -> String {
        "INSERT INTO events (aggregate_id, aggregate_type_id, version, event_type_id, data, metadata) VALUES ($1, $2, $3, $4, $5, $6)"
        .to_string()
    }

    fn insert_snapshot(&self) -> String {
        "INSERT INTO snapshots (aggregate_id, aggregate_type_id, version, data) VALUES ($1, $2, $3, $4)"
        .to_string()
    }

    fn get_events(&self) -> String {
        "SELECT id, aggregate_id, aggregate_type_id, version, event_type_id, data, metadata FROM events WHERE aggregate_id = $1 AND version > $2 ORDER BY version ASC;"
        .to_string()
    }

    fn get_snapshot(&self) -> String {
        "SELECT id, aggregate_id, aggregate_type_id, version, data FROM snapshots WHERE aggregate_id = $1 ORDER BY version DESC LIMIT 1;"
        .to_string()
    }

}


struct SqliteBuilder;

impl QueryBuilder for SqliteBuilder {
    fn build_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();

        let q = "CREATE TABLE IF NOT EXISTS aggregate_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            UNIQUE(name)
        );";
        queries.push(q.to_string());
        
        let q = "CREATE TABLE IF NOT EXISTS event_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            UNIQUE(name)
        );";
        queries.push(q.to_string());

        let q = "CREATE TABLE IF NOT EXISTS aggregate_instances (
            id INTEGER PRIMARY KEY,
            aggregate_type_id INTEGER NOT NULL,
            natural_key TEXT,
            UNIQUE(aggregate_type_id, natural_key),
            FOREIGN KEY(aggregate_type_id) REFERENCES aggregate_types(id)
        );";
        queries.push(q.to_string());

        let q = "CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            aggregate_id INTEGER NOT NULL,
            aggregate_type_id INTEGER NOT NULL,
            version INTEGER NOT NULL,
            event_type_id INTEGER NOT NULL,
            data TEXT NOT NULL,
            metadata TEXT,
            UNIQUE(aggregate_id, version),
            FOREIGN KEY(aggregate_id) REFERENCES aggregate_instances(id),
            FOREIGN KEY(aggregate_type_id) REFERENCES aggregate_types(id),
            FOREIGN KEY(event_type_id) REFERENCES event_types(id)
        );";
        queries.push(q.to_string());
        queries
    }

    fn insert_or_get_aggregate_type(&self) -> String {
        "INSERT INTO aggregate_types (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id;"
        .to_string()
    }

    fn insert_or_get_event_type(&self) -> String {
        "INSERT INTO event_types (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = $1 RETURNING id;"
        .to_string()
    }

    fn insert_aggregate_instance(&self) -> String {
        "INSERT INTO aggregate_instances (id, aggregate_type_id, natural_key) VALUES ($1, $2, $3)"
        .to_string()
    }

    fn insert_event(&self) -> String {
        "INSERT INTO events (aggregate_id, aggregate_type_id, version, event_type_id, data, metadata) VALUES ($1, $2, $3, $4, $5, $6)"
        .to_string()
    }

    fn insert_snapshot(&self) -> String {
        "INSERT INTO snapshots (aggregate_id, aggregate_type_id, version, data) VALUES ($1, $2, $3, $4)"
        .to_string()
    }

    fn get_events(&self) -> String {
        "SELECT id, aggregate_id, aggregate_type_id, version, event_type_id, data, metadata FROM events WHERE aggregate_id = $1 AND version > $2 ORDER BY version ASC;"
        .to_string()
    }

    fn get_snapshot(&self) -> String {
        "SELECT id, aggregate_id, aggregate_type_id, version, data FROM snapshots WHERE aggregate_id = $1 ORDER BY version DESC LIMIT 1;"
        .to_string()
    }

}




pub struct SqlxStorageEngine {
    dbtype: DbType,
    pool: sqlx::AnyPool,
    aggregate_types: Arc<Mutex<HashMap<String, i64>>>,
    event_types: Arc<Mutex<HashMap<String, i64>>>,
    query_builder: Arc<dyn QueryBuilder + Send + Sync>,
}

impl SqlxStorageEngine {
/// Creates a new SqlxStorageEngine.
    pub async fn new(dbtype: DbType, connection: &str) -> Result<SqlxStorageEngine, EventStoreError> {
        let pool = AnyPool::connect(connection).await.map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;

        let event_types: HashMap<String, i64> = HashMap::new();
        let event_types = Arc::new(Mutex::new(event_types));

        let aggregate_types: HashMap<String, i64> = HashMap::new();
        let aggregate_types = Arc::new(Mutex::new(aggregate_types));

        let query_builder: Arc<dyn QueryBuilder + Send + Sync> = match dbtype {
            DbType::Postgres => Arc::new(PostgresqlBuilder),
            DbType::Sqlite => Arc::new(SqliteBuilder),
        };


        Ok(SqlxStorageEngine {
            dbtype,
            pool,
            event_types,
            aggregate_types,
            query_builder,
        })
    }

    async fn get_connection(&self) -> Result<PoolConnection<sqlx::Any>, EventStoreError> {
        let connection = self.pool
            .acquire().await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
            Ok(connection)
    }

    /// Can be called to build the database schema.
    pub async fn build(&self) -> Result<(), EventStoreError> {
        let mut connection = self.get_connection().await?; 
        let queries = self.query_builder.build_queries();
        for query in queries {
            sqlx::query(&query).execute(&mut connection).await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
        }
        Ok(())
    }

    pub async fn get_aggregate_type_id(&self, aggregate_type: &str) -> Result<i64, EventStoreError> {
        let mut aggregate_types = self.aggregate_types.lock().await;
        if let Some(id) = aggregate_types.get(aggregate_type) {
            return Ok(*id);
        }

        let mut connection = self.get_connection().await?;
        let row = sqlx::query(&self.query_builder.insert_or_get_aggregate_type())
            .bind(aggregate_type)
            .fetch_one(&mut connection)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

        let id: i64 = row.get(0);

        // Why are unsigned values not allowed?!?
        let id: i64 = id.try_into().map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;
        aggregate_types.insert(aggregate_type.to_string(), id);
        Ok(id)
    }
    
    pub async fn get_event_type_id(&self, event_type: &str) -> Result<i64, EventStoreError> {
        let mut event_types = self.event_types.lock().await;
        if let Some(id) = event_types.get(event_type) {
            return Ok(*id);
        }

        let mut connection = self.get_connection().await?;
        let row = sqlx::query(&self.query_builder.insert_or_get_event_type())
            .bind(event_type)
            .fetch_one(&mut connection)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

        let id: i64 = row.get(0);

        // Why are unsigned values not allowed?!?
        let id: i64 = id.try_into().map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;
        event_types.insert(event_type.to_string(), id);
        Ok(id)
    }
}


#[async_trait::async_trait]
impl EventStoreStorageEngine for SqlxStorageEngine {
    async fn next_aggregate_id(&self, aggregate_type: &str, natural_key: Option<&str>) -> Result<i64, EventStoreError> {
        todo!();
    }

    async fn get_events(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
        version: i64,
    ) -> Result<Vec<Event>, EventStoreError> {
        todo!()
        /*
        let mut connection = self.get_connection().await?;
        let rows = sqlx::query(&self.query_builder.get_events())
            .bind(aggregate_id)
            .bind(aggregate_type)
            .bind(version)
            .fetch_all(&mut connection)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
        */

    }

    async fn get_snapshot(
        &self,
        aggregate_id: i64,
        aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        todo!();
    }

    async fn write_updates(
        &self,
        _events: &[Event],
        _snapshots: &[Snapshot],
    ) -> Result<(), EventStoreError> {
        let mut connection = self.get_connection().await?;

        for event in _events {
            let event_type_id = self.get_event_type_id(&event.event_type).await?;
            let aggregate_type_id = self.get_aggregate_type_id(&event.aggregate_type).await?;

            let aggregate_id: i64 = event.aggregate_id.try_into().map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
            let version: i64 = event.version.try_into().map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

            sqlx::query(&self.query_builder.insert_event())
                .bind(aggregate_id)
                .bind(aggregate_type_id)
                .bind(event_type_id)
                .bind(version)
                .bind(&event.data)
                .bind(&event.metadata)
                .execute(&mut connection)
                .await
                .map_err(|e| {
                    EventStoreError::StorageEngineError(Box::new(e))
                })?;
        }

        // Write snapshots
        for snapshot in _snapshots {
            let aggregate_type_id = self.get_aggregate_type_id(&snapshot.aggregate_type).await?;

            let aggregate_id: i64 = snapshot.aggregate_id.try_into().map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

            sqlx::query(&self.query_builder.insert_snapshot())
                .bind(aggregate_id)
                .bind(aggregate_type_id)
                .bind(&snapshot.data)
                .execute(&mut connection)
                .await
                .map_err(|e| {
                    EventStoreError::StorageEngineError(Box::new(e))
                })?;
        }

        Ok(())
    }
}




#[cfg(test)]
mod tests {
    #[test]
    fn test_stuff() {

        /*

        println!("Sqlite: {}", SchemaBuilder::aggregate_table(DbType::Sqlite));
        println!("Postgres: {}", SchemaBuilder::aggregate_table(DbType::Postgres));
        println!("Mysql: {}", SchemaBuilder::aggregate_table(DbType::Mysql));
        */
    }
}
