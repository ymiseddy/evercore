use crate::QueryBuilder;


pub struct SqliteBuilder;

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

    fn drop_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();

        let q = "DROP TABLE IF EXISTS events;";
        queries.push(q.to_string());

        let q = "DROP TABLE IF EXISTS aggregate_instances;";
        queries.push(q.to_string());

        let q = "DROP TABLE IF EXISTS event_types;";
        queries.push(q.to_string());

        let q = "DROP TABLE IF EXISTS aggregate_types;";
        queries.push(q.to_string());

        queries
    }
    
    fn insert_event_type(&self) -> String {
        "INSERT INTO event_types (name) VALUES (?);".to_string() 
    }

    fn get_event_type(&self) -> String {
        "SELECT id FROM event_types WHERE name = ?;".to_string() 
    }

    fn insert_aggregate_type(&self) -> String {
        "INSERT INTO aggregate_types (name) VALUES (?);".to_string() 
    }

    fn get_aggregate_type(&self) -> String {
        "SELECT id FROM aggregate_types WHERE name = ?;".to_string() 
    }

    fn insert_aggregate_instance(&self) -> String {
        "INSERT INTO aggregate_instances (aggregate_type_id, natural_key) VALUES ($1, $2) RETURNING id;"
        .to_string()
    }
    
    fn get_aggregate_instance_id(&self) -> String {
        "SELECT id FROM aggregate_instances WHERE aggregate_type_id = $1 AND natural_key = $2;"
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


