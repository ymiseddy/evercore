
use std::sync::Arc;
use eventide::EventStore;
use full_implementation::account_example;
use struct_backed_aggregate::user_example;

mod full_implementation;
mod struct_backed_aggregate;



#[tokio::main]
async fn main() {
    //  using memory storage engine for examples and tests.
    let memory = eventide::memory::MemoryStorageEngine::new();


    let event_store = EventStore::new(Arc::new(memory));
    let event_store = Arc::new(event_store);

    account_example(&event_store.clone()).await;
    user_example(&event_store.clone()).await;
}
