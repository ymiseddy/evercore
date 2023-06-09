use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::EventStoreError;

/// Event is a representation of a change in the aggregate state.
#[derive(Clone, Debug)]
pub struct Event {
    pub aggregate_id: i64,
    pub aggregate_type: String,
    pub version: i64,
    pub event_type: String,
    pub data: String,
    pub metadata: Option<String>
}

impl Event {
    pub fn new<T>(
        aggregate_id: i64, 
        aggregate_type: &str, 
        version: i64, 
        event_type: &str, 
        data: &T) -> Result<Event, EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        let state = serde_json::to_string(&data).map_err(EventStoreError::EventSerializationError)?;
        
        Ok(Event {
            aggregate_id,
            aggregate_type: aggregate_type.to_string(),
            version,
            event_type: event_type.to_string(),
            data: state,
            metadata: None
        })
    }

    pub fn add_metadata<T>(&mut self, metadata: &T) -> Result<(), EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        let state = serde_json::to_string(&metadata).map_err(EventStoreError::EventMetaDataSerializationError)?;
        self.metadata = Some(state);
        Ok(())
    }


    pub fn deserialize_metadata<T>(&self) -> Result<Option<T>, EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        match &self.metadata {
            Some(metadata) => serde_json::from_str(metadata).map_err(EventStoreError::EventDeserializationError),
            None => Ok(None)
        }
    }


    pub fn deserialize<T>(&self) -> Result<T, EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        serde_json::from_str(&self.data).map_err(EventStoreError::EventDeserializationError)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct SampleState {
        value: i64,
        name: String,
    }

    #[test]
    fn test_event_create() {

        let state = SampleState {
            value: 1,
            name: "test".to_string(),
        };

        let event = super::Event::new(1, "test", 1, "test", &state).unwrap();

        assert_eq!(event.aggregate_id, 1);
        assert_eq!(event.aggregate_type, "test");
        assert_eq!(event.version, 1);
        assert_eq!(event.event_type, "test");
        assert_eq!(event.data, "{\"value\":1,\"name\":\"test\"}");

    }

    #[test]
    fn test_event_deserialize() {

        let state = SampleState {
            value: 1,
            name: "test".to_string(),
        };

        let event = super::Event::new(1, "test", 1, "test", &state).unwrap();

        let deserialized: SampleState = event.deserialize().unwrap();

        assert_eq!(deserialized.value, 1);
        assert_eq!(deserialized.name, "test");
    }
}

