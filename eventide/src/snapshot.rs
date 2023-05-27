use serde::{Serialize, de::DeserializeOwned};
use crate::EventStoreError;

/// Snapshot is a representation of the aggregate state at a given point in time.
#[derive(Clone)]
pub struct Snapshot {
    pub aggregate_id: u64,
    pub aggregate_type: String,
    pub version: u64,
    pub data: String,
}

impl Snapshot {
    pub fn new<T>(aggregate_id: u64, aggregate_type: &str, version: u64, data: &T) -> Result<Snapshot, EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        let state = serde_json::to_string(&data).map_err(EventStoreError::SnapshotSerializationError)?;
        
        Ok(Snapshot {
            aggregate_id,
            aggregate_type: aggregate_type.to_string(),
            version,
            data: state,
        })
    }

    pub fn to_state<T>(&self) -> Result<T, EventStoreError>
        where T: Serialize + DeserializeOwned
    {
        serde_json::from_str(&self.data).map_err(EventStoreError::SnapshotDeserializationError)
    }
}


#[cfg(test)]
mod tests {
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug)]
    struct SampleState {
        value: u64,
        name: String,
    }

    #[test]
    fn test_snapshot_create() {

        let state = SampleState {
            value: 1,
            name: "test".to_string(),
        };

        let snapshot = super::Snapshot::new(1, "test", 1, &state).unwrap();

        assert_eq!(snapshot.aggregate_id, 1);
        assert_eq!(snapshot.aggregate_type, "test");
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.data, "{\"value\":1,\"name\":\"test\"}");

    }

    #[test]
    fn test_snapshot_deserialize() {

        let state = SampleState {
            value: 1,
            name: "test".to_string(),
        };

        let snapshot = super::Snapshot::new(1, "test", 1, &state).unwrap();

        let deserialized: SampleState = snapshot.to_state().unwrap();

        assert_eq!(deserialized.value, 1);
        assert_eq!(deserialized.name, "test");
    }
}

