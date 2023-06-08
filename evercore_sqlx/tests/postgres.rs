use std::sync::Mutex;
mod common;
use evercore_sqlx::{SqlxStorageEngine, DbType};
use sqlx::AnyPool;

// Postgres
const DATABASE_URL: &str = "postgres://dbtest:dbtest@localhost:5432/dbtest";
const DATABASE_TYPE: DbType = DbType::Postgres;

struct Initialization {
    pool: sqlx::AnyPool
}

static mut INITIALIZATION: Mutex<Option<Initialization>> = Mutex::new(None);


async fn get_initialized_pool() -> sqlx::AnyPool {

    unsafe {
        let mut initialization = INITIALIZATION.lock().unwrap();
        let pool = match &*initialization {
            Some(init) => init.pool.clone(),
            None => {
                let pool = AnyPool::connect(DATABASE_URL).await.unwrap();
                
                let storage = SqlxStorageEngine::new(DATABASE_TYPE, pool.clone());
                storage.drop_tables().await.unwrap();
                storage.build_tables().await.unwrap();


                let result_pool = pool.clone();
                *initialization = Some(Initialization {
                    pool,
                });
                result_pool
            }
        };
        pool
    }
}


#[tokio::test]
async fn ensure_can_add_new_aggregate_type() {
    let pool = get_initialized_pool().await;
    common::can_add_new_aggregate_type(DATABASE_TYPE, pool).await;
}

#[tokio::test]
async fn ensure_retrieves_existing_aggregate_without_cache() {
    let pool = get_initialized_pool().await;
    common::retrieves_existing_aggregate_without_cache(DATABASE_TYPE, pool).await;
}

#[tokio::test]
async fn ensure_can_create_new_event_type() {
    let pool = get_initialized_pool().await;
    common::can_create_new_event_type(DATABASE_TYPE, pool).await;
}

#[tokio::test]
async fn ensure_can_create_new_event_type_without_cache() {
    let pool = get_initialized_pool().await;
    common::can_create_new_event_type_without_cache(DATABASE_TYPE, pool).await;
}

#[tokio::test]
async fn ensure_can_create_new_aggregate_instance() {
    let pool = get_initialized_pool().await;
    common::can_create_new_aggregate_instance(DATABASE_TYPE, pool).await;
}


#[tokio::test]
async fn ensure_can_write_updates() {
    let pool = get_initialized_pool().await;
    common::can_write_updates(DATABASE_TYPE, pool).await;
}

