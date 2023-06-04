use std::sync::Mutex;

use eventide::EventStoreStorageEngine;
use eventide_sqlx::{SqlxStorageEngine, DbType};

// Postgres
const DATABASE_URL: &str = "postgres://admin:admin@localhost:5432/main";
const DATABASE_TYPE: DbType = DbType::Postgres;

// Sqlite - Note: Run with threads=1 or else you get a "database is locked" error
// const DATABASE_URL: &str = "sqlite://test.db?mode=rwc";
//const DATABASE_TYPE: DbType = DbType::Sqlite;

// Mysql
// const DATABASE_URL: &str = "mysql://dbtest:dbtest@localhost/dbtest";
// const DATABASE_TYPE: DbType = DbType::Mysql;

static mut INITIALIZED: Mutex<bool>  = Mutex::new(false);

async fn initialize() {

    unsafe {
        let mut initialized = INITIALIZED.lock().unwrap();

        if !*initialized {
            println!("Initializing database");
            let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
            storage.drop().await.unwrap();
            storage.build().await.unwrap();
            println!("Initializing database complete");
        }

        if *initialized {
            return;
        }
        *initialized = true;
    }
}


#[tokio::test]
async fn ensure_can_add_new_aggregate_type() {
    initialize().await;
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let aggregate_type_id = storage.get_aggregate_type_id("test_aggregate").await.unwrap();

    let aggregate_type_id_after = storage.get_aggregate_type_id("test_aggregate").await.unwrap();
    assert_eq!(aggregate_type_id, aggregate_type_id_after);
}

#[tokio::test]
async fn ensure_retrieves_existing_aggregate_without_cache() {
    initialize().await;
    
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let aggregate_type_id = storage.get_aggregate_type_id("test_aggregate2").await.unwrap();

    // Re-instantiate to bypass cache.
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let aggregate_type_id_after = storage.get_aggregate_type_id("test_aggregate2").await.unwrap();
    assert_eq!(aggregate_type_id, aggregate_type_id_after);
}

#[tokio::test]
async fn ensure_can_create_new_event_type() {
    initialize().await;
    
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let event_type_id = storage.get_event_type_id("test_event").await.unwrap();
    let event_type_id_after = storage.get_event_type_id("test_event").await.unwrap();
    assert_eq!(event_type_id, event_type_id_after);
}


#[tokio::test]
async fn ensure_can_create_new_event_type_without_cache() {
    initialize().await;
    
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let event_type_id = storage.get_event_type_id("test_event2").await.unwrap();
    
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let event_type_id_after = storage.get_event_type_id("test_event2").await.unwrap();

    assert_eq!(event_type_id, event_type_id_after);
}

#[tokio::test]
async fn ensure_can_create_new_aggregate_instance() {
    initialize().await;
    
    let storage = SqlxStorageEngine::new(DATABASE_TYPE, DATABASE_URL).await.unwrap();
    let aggregate_instance = storage.create_aggregate_instance("user", Some("roger.test@example.com")).await.unwrap();
    let aggregate_instance_retrieved = storage.get_aggregate_instance_id("user", "roger.test@example.com").await.unwrap().unwrap();

    assert!(aggregate_instance > 0);
    assert_eq!(aggregate_instance, aggregate_instance_retrieved);
}
