use crate::QueryBuilder;

pub(crate) struct MysqlBuilder;

impl QueryBuilder for MysqlBuilder {
    fn build_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();
        queries.push(String::from("CREATE TABLE IF NOT EXISTS aggregate_types (
            id BIGINT NOT NULL AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY (name)
        )"));

        queries.push(String::from("CREATE TABLE IF NOT EXISTS event_types (
            id BIGINT NOT NULL AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY (name)
        )"));

        queries.push(String::from("CREATE TABLE IF NOT EXISTS aggregate_instance (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_type_id BIGINT NOT NULL,
            natural_key VARCHAR(255),
            PRIMARY KEY (id),
            UNIQUE KEY (aggregate_type_id, natural_key),
            CONSTRAINT fk_aggregate_instance_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        )"));

        queries.push(String::from("CREATE TABLE IF NOT EXISTS event (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            event_type_id BIGINT NOT NULL,
            data JSON NOT NULL,
            metadata JSON,
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
        )"));

        queries.push(String::from("CREATE TABLE IF NOT EXISTS snapshot (
            id BIGINT NOT NULL AUTO_INCREMENT,
            aggregate_id BIGINT NOT NULL,
            aggregate_type_id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            data JSON NOT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY (aggregate_id, version),
            CONSTRAINT fk_snapshot_aggregate_id
                FOREIGN KEY(aggregate_id)
                    REFERENCES aggregate_instance(id),
            CONSTRAINT fk_snapshot_aggregate_type_id
                FOREIGN KEY(aggregate_type_id)
                    REFERENCES aggregate_types(id)
        )"));

        queries
    }

    fn drop_queries(&self) -> Vec<String> {
        let mut queries = Vec::new();
        queries.push(String::from("DROP TABLE IF EXISTS snapshot"));
        queries.push(String::from("DROP TABLE IF EXISTS event"));
        queries.push(String::from("DROP TABLE IF EXISTS aggregate_instance"));
        queries.push(String::from("DROP TABLE IF EXISTS event_types"));
        queries.push(String::from("DROP TABLE IF EXISTS aggregate_types"));
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
        "INSERT INTO aggregate_instance (aggregate_type_id, natural_key) VALUES (?, ?)".to_string() 
    }

    fn insert_event(&self) -> String {
        todo!()
    }

    fn insert_snapshot(&self) -> String {
        todo!()
    }

    fn get_events(&self) -> String {
        todo!()
    }

    fn get_snapshot(&self) -> String {
        todo!()
    }

    fn get_aggregate_instance_id(&self) -> String {
        "SELECT id FROM aggregate_instance WHERE aggregate_type_id = ? AND natural_key = ?".to_string()
    }
}



