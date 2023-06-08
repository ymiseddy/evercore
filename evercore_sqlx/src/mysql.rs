use crate::QueryBuilder;

pub(crate) struct MysqlBuilder;

impl QueryBuilder for MysqlBuilder {
    fn build_queries(&self) -> Vec<String> {
        vec![
            String::from("CREATE TABLE IF NOT EXISTS aggregate_types (
                id BIGINT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                PRIMARY KEY (id),
                UNIQUE KEY (name)
            )"),
            String::from("CREATE TABLE IF NOT EXISTS event_types (
                id BIGINT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                PRIMARY KEY (id),
                UNIQUE KEY (name)
            )"),
        String::from("CREATE TABLE IF NOT EXISTS aggregate_instance (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_type_id BIGINT NOT NULL,
            natural_key VARCHAR(255),
            PRIMARY KEY (id),
            UNIQUE KEY (aggregate_type_id, natural_key),
            CONSTRAINT fk_aggregate_instance_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        )"),

        String::from("CREATE TABLE IF NOT EXISTS events (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            event_type_id BIGINT NOT NULL,
            data TEXT NOT NULL,
            metadata TEXT,
            PRIMARY KEY (id),
            UNIQUE KEY (aggregate_id, version),
            CONSTRAINT fk_event_aggregate_id
                FOREIGN KEY(aggregate_id)
                    REFERENCES aggregate_instance(id),
            CONSTRAINT fk_event_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id),
            CONSTRAINT fk_event_type_id
                FOREIGN KEY(event_type_id)
                    REFERENCES event_types(id)
        )"),

        String::from("CREATE TABLE IF NOT EXISTS snapshots (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            data TEXT NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY (aggregate_id, version),
            CONSTRAINT fk_snapshot_aggregate_id
                FOREIGN KEY(aggregate_id)
                    REFERENCES aggregate_instance(id),
            CONSTRAINT fk_snapshot_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        )"),
        ]
    }

    fn drop_queries(&self) -> Vec<String> {
        vec![
            String::from("DROP TABLE IF EXISTS snapshots"),
            String::from("DROP TABLE IF EXISTS events"),
            String::from("DROP TABLE IF EXISTS aggregate_instance"),
            String::from("DROP TABLE IF EXISTS aggregate_types"),
            String::from("DROP TABLE IF EXISTS event_types"),
        ] 
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
        "INSERT INTO aggregate_instance (aggregate_type_id, natural_key) VALUES (?, ?)".to_string() 
    }

    fn insert_event(&self) -> String {
        "INSERT INTO events (aggregate_id, aggregate_type_id, version, event_type_id, data, metadata) VALUES (?, ?, ?, ?, ?, ?)".to_string()
    }

    fn insert_snapshot(&self) -> String {
        "INSERT INTO snapshots (aggregate_id, aggregate_type_id, version, data) VALUES (?, ?, ?, ?)".to_string()
    }
    
    fn get_events(&self) -> String {
        "SELECT aggregate_id, aggregate_types.name AS aggregate_type, 
         version, event_types.name AS event_type, data, metadata 
         FROM events 
         LEFT JOIN aggregate_types ON aggregate_types.id = events.aggregate_type_id
         LEFT JOIN event_types ON event_types.id = events.event_type_id
         WHERE aggregate_id = ? AND aggregate_type_id = ? AND version > ? ORDER BY version ASC;"
        .to_string()
    }

    fn get_snapshot(&self) -> String {
        "SELECT aggregate_id, aggregate_types.name as aggregate_type, version, data 
         FROM snapshots 
         LEFT JOIN aggregate_types ON aggregate_types.id = snapshots.aggregate_type_id
         WHERE aggregate_id = ? AND aggregate_type_id = ? ORDER BY version DESC LIMIT 1;"
        .to_string()
    }

    fn get_aggregate_instance_id(&self) -> String {
        "SELECT id FROM aggregate_instance WHERE aggregate_type_id = ? AND natural_key = ?".to_string()
    }
}



