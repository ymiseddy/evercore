use std::sync::PoisonError;

use thiserror::Error;

/// EventStoreError is the error type for the event store.
#[derive(Error, Debug)]
pub enum EventStoreError {

    #[error("Aggregate not found: {0:?}")]
    AggregateNotFound((String, i64)),

    #[error("Error serializaing event.")]
    EventSerializationError(serde_json::Error),
    
    #[error("Error serializaing metadata for event.")]
    EventMetaDataSerializationError(serde_json::Error),

    #[error("Error deserializaing event.")]
    EventDeserializationError(serde_json::Error),

    #[error("Error serializaing snapshot.")]
    SnapshotSerializationError(serde_json::Error),

    #[error("Error deserializaing snapshot.")]
    SnapshotDeserializationError(serde_json::Error),

    #[error("Error saving events.")]
    SaveEventsError(Box<dyn std::error::Error>),

    #[error("Error saving snapshot.")]
    SaveSnapshotError(Box<dyn std::error::Error>),

    #[error("Error getting events.")]
    GetEventsError(Box<dyn std::error::Error>),

    #[error("Error getting snapshot.")]
    GetSnapshotError(Box<dyn std::error::Error>),

    #[error("Error getting next aggregate id.")]
    GetNextAggregateIdError(Box<dyn std::error::Error>),

    #[error("Error applying snapshot.")]
    ApplySnapshotError(String),

    #[error("Error processing request.")]
    RequestProcessingError(String),

    #[error("Error applying event.")]
    ApplyEventError(String),

    #[error("Error during context callback.")]
    ContextError(Box<dyn std::error::Error>),

    /*
    #[error("Error acquiring lock in context.")]
    ContextErrorLock(#[from] PoisonError),
    */
    #[error("Error acquiring lock in context.")]
    ContextPoisonError,
    
    #[error("Error in storage engine.")]
    ContextErrorOther(String),

    #[error("Attempt to publish an event before context is set.")]
    NoContext,

    #[error("Error in storage engine.")]
    StorageEngineError(Box<dyn std::error::Error>),
   
    #[error("Error in storage engine.")]
    StorageEngineErrorOther(String),
    
    #[error("Error in storage engine.")]
    StorageEngineConnectionError(String),

    #[error("Aggregate instance not found.")]
    AggregateInstanceNotFound,

}


impl<T> From<PoisonError<T>> for EventStoreError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::ContextPoisonError
    }
}


