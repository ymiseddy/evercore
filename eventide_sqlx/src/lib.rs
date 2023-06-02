use eventide::{event::Event, snapshot::Snapshot, EventStoreError, EventStoreStorageEngine};
use sea_query::{
    ColumnDef, Iden, MysqlQueryBuilder, PostgresQueryBuilder, SqliteQueryBuilder, Table,
};
use sqlx::{AnyPool, pool::PoolConnection};


#[derive(Copy, Clone, Iden, Debug)]
enum AggregateTable {
    #[iden = "aggregate"]
    Table,
    Id,
    Type,
    NaturalKey,
}

#[derive(Iden)]
enum AggregateTypesTable {
    #[iden = "aggregate_type"]
    Table,
    Id,
    Type,
}

#[derive(Iden)]
enum EventTypesTable {
    #[iden = "event_type"]
    Table,
    Id,
    Type,
}

#[derive(Iden)]
enum EventTable {
    #[iden = "event"]
    Table,
    Id,
    AggregateId,
    AggregateTypeId,
    AggregateVersion,
    EventTypeId,
    EventData,
    EventMetadata,
    EventTime,
}

#[derive(Iden)]
enum SnapshotTable {
    #[iden = "snapshot"]
    Table,
    Id,
    AggregateId,
    AggregateTypeId,
    AggregateVersion,
    SnapshotData,
    SnapshotMetadata,
    SnapshotTime,
}

struct SqlxStorageEngine {
    dbtype: DbType,
    connection: String,
    pool: sqlx::AnyPool,
}

impl SqlxStorageEngine {
    pub async fn new(dbtype: DbType, connection: &str) -> Result<SqlxStorageEngine, EventStoreError> {
        let pool = AnyPool::connect(connection).await.map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;
        Ok(SqlxStorageEngine {
            dbtype,
            connection: connection.to_string(),
            pool,
        })
    }

    async fn get_connection(&self) -> Result<PoolConnection<sqlx::Any>, EventStoreError> {
        let connection = self.pool
            .acquire().await.map_err(|e| {
                EventStoreError::StorageEngineError(Box::new(e))
            })?;
            Ok(connection)
    }

    pub async fn build(&self, dbtype: DbType) -> Result<(), EventStoreError> {
        let mut connection = self.get_connection().await?;
        let schema_builder = SchemaBuilder::new(&dbtype);

        let query = schema_builder.aggregate_types_table().to_string();
        sqlx::query(&query).execute(&mut connection).await.map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;

        let query = schema_builder.aggregate_table().to_string();
        sqlx::query(&query).execute(&mut connection).await.map_err(|e| {
            EventStoreError::StorageEngineError(Box::new(e))
        })?;


        Ok(())
    }
}

#[async_trait::async_trait]
impl EventStoreStorageEngine for SqlxStorageEngine {
    async fn next_aggregate_id(&self) -> Result<u64, EventStoreError> {
        todo!();
    }

    async fn get_events(
        &self,
        _aggregate_id: u64,
        _aggregate_type: &str,
        _version: u64,
    ) -> Result<Vec<Event>, EventStoreError> {
        todo!();
    }

    async fn get_snapshot(
        &self,
        _aggregate_id: u64,
        _aggregate_type: &str,
    ) -> Result<Option<Snapshot>, EventStoreError> {
        todo!();
    }

    async fn write_updates(
        &self,
        _events: &[Event],
        _snapshots: &[Snapshot],
    ) -> Result<(), EventStoreError> {
        todo!();
    }
}

pub enum DbType {
    Sqlite,
    Postgres,
    Mysql,
}

struct QueryBuilders {
    schema: Box<dyn sea_query::SchemaBuilder>,
    query: Box<dyn sea_query::QueryBuilder>,
}

fn get_builders(dbtype: &DbType) -> QueryBuilders {
    match dbtype {
        DbType::Sqlite => QueryBuilders {
            schema: Box::new(SqliteQueryBuilder),
            query: Box::new(SqliteQueryBuilder),
        },
        DbType::Postgres => QueryBuilders {
            schema: Box::new(PostgresQueryBuilder),
            query: Box::new(PostgresQueryBuilder),
        },
        DbType::Mysql => QueryBuilders {
            schema: Box::new(MysqlQueryBuilder),
            query: Box::new(MysqlQueryBuilder),
        },
    }
}

struct SchemaBuilder {
    query_builders: QueryBuilders,
}

impl SchemaBuilder {
    fn new(dbtype: &DbType) -> SchemaBuilder {
        SchemaBuilder {
            query_builders: get_builders(dbtype),
        }
    }

    fn aggregate_table(&self) -> String {
        let table = Table::create()
            .table(AggregateTable::Table)
            .col(
                ColumnDef::new(AggregateTable::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(AggregateTable::Type).string().not_null())
            .col(
                ColumnDef::new(AggregateTable::NaturalKey)
                    .string()
                    .string_len(64)
                    .unique_key(),
            )
            .to_owned();
        table.build_any(self.query_builders.schema.as_ref())
    }

    fn aggregate_types_table(&self) -> String {
        Table::create()
            .table(AggregateTypesTable::Table)
            .col(
                ColumnDef::new(AggregateTypesTable::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(AggregateTypesTable::Type)
                    .string()
                    .string_len(64)
                    .unique_key(),
            )
            .build_any(self.query_builders.schema.as_ref())
    }

    fn event_types_table(&self) -> String {
        Table::create()
            .table(EventTypesTable::Table)
            .col(
                ColumnDef::new(EventTypesTable::Id)
                    .integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(
                ColumnDef::new(EventTypesTable::Type)
                    .string()
                    .string_len(64)
                    .unique_key(),
            )
            .build_any(self.query_builders.schema.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stuff() {
        let schema_builder = SchemaBuilder::new(&DbType::Sqlite);
        println!("Sqlite: {}", schema_builder.aggregate_table());

        /*

        println!("Sqlite: {}", SchemaBuilder::aggregate_table(DbType::Sqlite));
        println!("Postgres: {}", SchemaBuilder::aggregate_table(DbType::Postgres));
        println!("Mysql: {}", SchemaBuilder::aggregate_table(DbType::Mysql));
        */
    }
}
