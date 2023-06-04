#[forbid(unsafe_code)]

mod pg;
mod sqlite;
mod mysql;
mod queries;

use crate::queries::QueryBuilder;
use std::{collections::HashMap, sync::Arc};
use futures::lock::Mutex;
use eventide::{event::Event, snapshot::Snapshot, EventStoreError, EventStoreStorageEngine};
use sqlx::{AnyPool, pool::PoolConnection, Row, Connection};
use pg::PostgresqlBuilder;
use sqlite::SqliteBuilder;
use mysql::MysqlBuilder;

pub enum DbType {
    Sqlite,
    Postgres,
    Mysql,
}




pub struct SqlxStorageEngine {
    pool: sqlx::AnyPool,
    aggregate_types: Arc<Mutex<HashMap<String, i64>>>,
    event_types: Arc<Mutex<HashMap<String, i64>>>,
    query_builder: Arc<dyn QueryBuilder + Send + Sync>,
    dbtype: DbType,
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
            DbType::Mysql => Arc::new(MysqlBuilder),
        };


        Ok(SqlxStorageEngine {
            pool,
            event_types,
            aggregate_types,
            query_builder,
            dbtype,
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

    pub async fn drop(&self) -> Result<(), EventStoreError> {
        let mut connection = self.get_connection().await?; 
        let queries = self.query_builder.drop_queries();
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
        let mut tx = connection.begin()
            .await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

        let query = self.query_builder.get_aggregate_type();
        let row = sqlx::query(&query)
            .bind(aggregate_type)
            .fetch_optional(&mut tx)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

            let id = match row {
                Some(row) => {
                    let id: i64 = row.get(0);
                    // Why are unsigned values not allowed?!?
                    let id: i64 = id.try_into().map_err(|e| {
                        EventStoreError::StorageEngineError(Box::new(e))
                    })?;
                    id
                },
                None => {
                    let query = self.query_builder.insert_aggregate_type();
                    let query = sqlx::query(&query)
                        .bind(aggregate_type);

                    let id = match &self.dbtype {
                        DbType::Postgres => {
                            let result = query
                                .fetch_one(&mut tx)
                                .await
                                .map_err(|e| {
                                    EventStoreError::StorageEngineError(Box::new(e))
                                })?;
                            result.get(0)
                        },
                        _ => {
                            let result = query.execute(&mut tx)
                                .await
                                .map_err(|e| {
                                    EventStoreError::StorageEngineError(Box::new(e))
                                })?;

                            result.last_insert_id()
                                .ok_or_else(|| EventStoreError::StorageEngineErrorOther("Couldn't retrieve last insert id.".to_string()))?
                        }
                    };
                    id
                }
            };
            tx.commit().await.map_err(|e| {
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
        let mut tx = connection.begin()
            .await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

        let query = self.query_builder.get_event_type();

        let row = sqlx::query(&query)
            .bind(event_type)
            .fetch_optional(&mut tx)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

            let id = match row {
                Some(row) => {
                    let id: i64 = row.get(0);
                    // Why are unsigned values not allowed?!?
                    let id: i64 = id.try_into().map_err(|e| {
                        EventStoreError::StorageEngineError(Box::new(e))
                    })?;
                    id
                },
                None => {
                    let query = self.query_builder.insert_event_type();
                    let query = sqlx::query(&query)
                        .bind(event_type);

                    let id = match &self.dbtype {
                        DbType::Postgres => {
                            let result = query
                                .fetch_one(&mut tx)
                                .await
                                .map_err(|e| {
                                    EventStoreError::StorageEngineError(Box::new(e))
                                })?;
                            result.get(0)
                        },
                        _ => {
                            let result = query.execute(&mut tx)
                                .await
                                .map_err(|e| {
                                    EventStoreError::StorageEngineError(Box::new(e))
                                })?;

                            result.last_insert_id()
                                .ok_or_else(|| EventStoreError::StorageEngineErrorOther("Couldn't retrieve last insert id.".to_string()))?
                        }
                    };
                    id
                }
            };
            tx.commit().await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
            event_types.insert(event_type.to_string(), id);
            Ok(id)
    }
}


#[async_trait::async_trait]
impl EventStoreStorageEngine for SqlxStorageEngine {
    async fn create_aggregate_instance(
        &self, 
        aggregate_type: &str, 
        natural_key: Option<&str>
    ) -> Result<i64, EventStoreError> {

        let aggregate_type_id = self.get_aggregate_type_id(aggregate_type).await?;

        let query = self.query_builder.insert_aggregate_instance();

        let mut connection = self.get_connection().await?;
        let query = sqlx::query(&query)
            .bind(aggregate_type_id)
            .bind(natural_key);

            let id = match &self.dbtype {
                DbType::Postgres => {
                    let result = query
                        .fetch_one(&mut connection)
                        .await
                        .map_err(|e| {
                            EventStoreError::StorageEngineError(Box::new(e))
                        })?;
                    result.get(0)
                },
                _ => {
                    let result = query.execute(&mut connection)
                        .await
                        .map_err(|e| {
                            EventStoreError::StorageEngineError(Box::new(e))
                        })?;

                      result.last_insert_id()
                        .ok_or_else(|| EventStoreError::StorageEngineErrorOther("Couldn't retrieve last insert id.".to_string()))?
                }
            };
        Ok(id)
    }

    async fn get_aggregate_instance_id(
        &self, 
        aggregate_type: &str, 
        natural_key: &str
    ) -> Result<Option<i64>, EventStoreError> {
        let aggregate_type_id = self.get_aggregate_type_id(aggregate_type).await?;
        let query = self.query_builder.get_aggregate_instance_id();

        let mut connection = self.get_connection().await?;
        let row = sqlx::query(&query)
            .bind(aggregate_type_id)
            .bind(natural_key)
            .fetch_optional(&mut connection)
            .await
            .map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;

        if let Some(row) = row {
            let id: i64 = row.get(0);
            Ok(Some(id))
        } else {
            Ok(None)
        }
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
