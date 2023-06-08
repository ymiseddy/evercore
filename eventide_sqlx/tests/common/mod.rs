use eventide::{EventStoreStorageEngine, event::Event, snapshot::Snapshot};
use eventide_sqlx::SqlxStorageEngine;
use serde::{Serialize, Deserialize};
use eventide_sqlx::DbType;

#[derive(Serialize, Deserialize, Debug)]
struct UserCreate {
    name: String,
    email: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Context {
    user_id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct UserState {
    name: String,
    email: String,
}



pub async fn can_add_new_aggregate_type(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype, pool);
    let aggregate_type_id = storage.get_aggregate_type_id("test_aggregate").await.unwrap();

    let aggregate_type_id_after = storage.get_aggregate_type_id("test_aggregate").await.unwrap();
    assert_eq!(aggregate_type_id, aggregate_type_id_after);
}

pub async fn retrieves_existing_aggregate_without_cache(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype.clone(), pool.clone());
    
    let aggregate_type_id = storage.get_aggregate_type_id("test_aggregate2").await.unwrap();

    // Re-instantiate to bypass cache.
    let storage = SqlxStorageEngine::new(dbtype, pool.clone());
    let aggregate_type_id_after = storage.get_aggregate_type_id("test_aggregate2").await.unwrap();
    assert_eq!(aggregate_type_id, aggregate_type_id_after);
}

pub async fn can_create_new_event_type(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype, pool);
    
    let event_type_id = storage.get_event_type_id("test_event").await.unwrap();
    let event_type_id_after = storage.get_event_type_id("test_event").await.unwrap();
    assert_eq!(event_type_id, event_type_id_after);
}

pub async fn can_create_new_event_type_without_cache(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype.clone(), pool.clone());
   
    let event_type_id = storage.get_event_type_id("test_event2").await.unwrap();
    
    let storage = SqlxStorageEngine::new(dbtype, pool.clone());
    let event_type_id_after = storage.get_event_type_id("test_event2").await.unwrap();

    assert_eq!(event_type_id, event_type_id_after);
}

pub async fn can_create_new_aggregate_instance(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype, pool);
    
    let aggregate_instance = storage.create_aggregate_instance("admin", Some("roger.test@example.com")).await.unwrap();
    let aggregate_instance_retrieved = storage.get_aggregate_instance_id("admin", "roger.test@example.com").await.unwrap().unwrap();

    assert!(aggregate_instance > 0);
    assert_eq!(aggregate_instance, aggregate_instance_retrieved);
}

pub async fn can_write_updates(dbtype: DbType, pool: sqlx::AnyPool) {
    let storage = SqlxStorageEngine::new(dbtype, pool);
    
    let aggregate_instance = storage.create_aggregate_instance("user", Some("sample.test@example.com")).await.unwrap();

    let user_created = UserCreate {
        name: "Sample".to_string(),
        email: "sample.test@example.com".to_string(),
    };

    let context = Context {
        user_id: 1734,
    };


    let mut event = Event::new(aggregate_instance, "user", 1, "created", &user_created).unwrap();
    event.add_metadata(&context).unwrap();
    let events = vec![event];

    let user_state = UserState {
        name: "Sample".to_string(),
        email: "sample.test@example.com".to_string(),
    };
    let snapshot = Snapshot::new(aggregate_instance, "user", 1, &user_state).unwrap();


    let snapshots: Vec<Snapshot> = vec![snapshot];
    storage.write_updates(&events, &snapshots).await.unwrap();

    let new_events = storage.read_events(aggregate_instance, "user", 0).await.unwrap();



    let new_snapshot = storage.read_snapshot(aggregate_instance, "user").await.unwrap()
        .unwrap();
    
    assert_eq!(new_events.len(), 1);
    assert_eq!(new_events[0].aggregate_id, events[0].aggregate_id);
    assert_eq!(new_events[0].aggregate_type, events[0].aggregate_type);
    assert_eq!(new_events[0].event_type, events[0].event_type);
    assert_eq!(new_events[0].version, events[0].version);
    assert_eq!(new_events[0].data, events[0].data);
    assert_eq!(new_events[0].metadata, events[0].metadata);

    assert_eq!(new_snapshot.aggregate_id, snapshots[0].aggregate_id);
    assert_eq!(new_snapshot.aggregate_type, snapshots[0].aggregate_type);
    assert_eq!(new_snapshot.version, snapshots[0].version);
    assert_eq!(new_snapshot.data, snapshots[0].data);
}



