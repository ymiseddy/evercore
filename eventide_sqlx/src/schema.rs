use sea_query::{
    ColumnDef, Iden, Table, ForeignKey, ForeignKeyAction,
};

use crate::{QueryBuilders, get_builders, DbType};


#[derive(Copy, Clone, Iden, Debug)]
pub enum AggregateTable {
    #[iden = "aggregate"]
    Table,
    Id,
    AggregateTypeId,
    NaturalKey,
}

#[derive(Iden)]
pub enum AggregateTypesTable {
    #[iden = "aggregate_type"]
    Table,
    Id,
    Type,
}

#[derive(Iden)]
pub enum EventTypesTable {
    #[iden = "event_type"]
    Table,
    Id,
    Type,
}

#[derive(Iden)]
pub enum EventTable {
    #[iden = "event"]
    Table,
    Id,
    AggregateId,
    AggregateTypeId,
    Version,
    EventTypeId,
    Data,
    Metadata,
    Time,
}

#[derive(Iden)]
pub enum SnapshotTable {
    #[iden = "snapshot"]
    Table,
    Id,
    AggregateId,
    AggregateTypeId,
    Version,
    Data,
    Time,
}

pub struct SchemaBuilder {
    query_builders: QueryBuilders,
}

impl SchemaBuilder {
    pub fn new(dbtype: &DbType) -> SchemaBuilder {
        SchemaBuilder {
            query_builders: get_builders(dbtype),
        }
    }

    pub fn create(&self) -> Vec<String> {
        let mut queries: Vec<String> = Vec::new();
        
        // AggregateTypes Table
        let q = Table::create()
            .table(AggregateTypesTable::Table)
            .col(
                ColumnDef::new(AggregateTypesTable::Id)
                    .unsigned().integer()
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
            .build_any(self.query_builders.schema.as_ref());
        queries.push(q);
        
        // EventTypes Table
        let q = Table::create()
            .table(EventTypesTable::Table)
            .col(
                ColumnDef::new(EventTypesTable::Id)
                    .unsigned().integer()
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
            .build_any(self.query_builders.schema.as_ref());
        queries.push(q);

        // Aggregate Table
        let q = Table::create()
            .table(AggregateTable::Table)
            .col(
                ColumnDef::new(AggregateTable::Id)
                    .unsigned().integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(AggregateTable::AggregateTypeId).integer().not_null())
            .col(
                ColumnDef::new(AggregateTable::NaturalKey)
                    .string()
                    .string_len(64)
                    .unique_key(),
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_aggregate_type")
                    .from(AggregateTable::Table, AggregateTable::AggregateTypeId)
                    .to(AggregateTypesTable::Table, AggregateTypesTable::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict)
            )
            .to_owned()
            .build_any(self.query_builders.schema.as_ref());
        queries.push(q);


        // Event Table
        let q = Table::create()
            .table(EventTable::Table)
            .col(
                ColumnDef::new(EventTable::Id)
                    .unsigned().integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(EventTable::AggregateId).unsigned().integer().not_null())
            .col(ColumnDef::new(EventTable::AggregateTypeId).unsigned().integer().not_null())
            .col(ColumnDef::new(EventTable::Version).unsigned().integer().not_null())
            .col(ColumnDef::new(EventTable::EventTypeId).unsigned().integer().not_null())
            .col(ColumnDef::new(EventTable::Data).string().not_null())
            .col(ColumnDef::new(EventTable::Metadata).string().not_null())
            .col(ColumnDef::new(EventTable::Time).date_time().not_null())
            .foreign_key(
                ForeignKey::create()
                    .name("fk_event_aggregate")
                    .from(EventTable::Table, EventTable::AggregateId)
                    .to(AggregateTable::Table, AggregateTable::Id)
                    .on_delete(ForeignKeyAction::Cascade)
                    .on_update(ForeignKeyAction::Cascade)
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_event_aggregate_type")
                    .from(EventTable::Table, EventTable::AggregateTypeId)
                    .to(AggregateTypesTable::Table, AggregateTypesTable::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict)
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_event_type")
                    .from(EventTable::Table, EventTable::EventTypeId)
                    .to(EventTypesTable::Table, EventTypesTable::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict)
            )
            .to_owned()
            .build_any(self.query_builders.schema.as_ref());
        queries.push(q);

        // Snapshot Table
        let q = Table::create()
            .table(SnapshotTable::Table)
            .col(
                ColumnDef::new(SnapshotTable::Id)
                    .unsigned().integer()
                    .not_null()
                    .auto_increment()
                    .primary_key(),
            )
            .col(ColumnDef::new(SnapshotTable::AggregateId).unsigned().integer().not_null())
            .col(ColumnDef::new(SnapshotTable::AggregateTypeId).unsigned().integer().not_null())
            .col(ColumnDef::new(SnapshotTable::Version).integer().not_null())
            .col(ColumnDef::new(SnapshotTable::Data).string().not_null())
            .col(ColumnDef::new(SnapshotTable::Time).date_time().not_null())
            .foreign_key(
                ForeignKey::create()
                    .name("fk_snapshot_aggregate")
                    .from(SnapshotTable::Table, SnapshotTable::AggregateId)
                    .to(AggregateTable::Table, AggregateTable::Id)
                    .on_delete(ForeignKeyAction::Cascade)
                    .on_update(ForeignKeyAction::Cascade)
            )
            .foreign_key(
                ForeignKey::create()
                    .name("fk_snapshot_aggregate_type")
                    .from(EventTable::Table, EventTable::AggregateTypeId)
                    .to(AggregateTypesTable::Table, AggregateTypesTable::Id)
                    .on_delete(ForeignKeyAction::Restrict)
                    .on_update(ForeignKeyAction::Restrict)
            )
            .to_owned()
            .build_any(self.query_builders.schema.as_ref());
        queries.push(q);

        queries
    }
}


