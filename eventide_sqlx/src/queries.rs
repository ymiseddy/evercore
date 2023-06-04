pub (crate) trait QueryBuilder {
    fn build_queries(&self) -> Vec<String>;
    fn drop_queries(&self) -> Vec<String>;
    fn insert_aggregate_type(&self) -> String;
    fn get_aggregate_type(&self) -> String;
    fn insert_event_type(&self) -> String;
    fn get_event_type(&self) -> String;
    fn insert_aggregate_instance(&self) -> String;
    fn insert_event(&self) -> String;
    fn insert_snapshot(&self) -> String;
    fn get_events(&self) -> String;
    fn get_snapshot(&self) -> String;
    fn get_aggregate_instance_id(&self) -> String;
}

