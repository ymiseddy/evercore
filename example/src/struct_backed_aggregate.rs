use std::sync::Arc;

use eventide::{EventStore, aggregate::{StructBackedAggregate, StructAggregateImpl, CanRequest, Aggregate}, EventStoreError, event::Event};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct User {
    name: String,
    email: String,
    password_hash: String,
}


#[derive(Serialize, Deserialize, Debug)]
enum UserRequests {
    Created {
        name: String,
        email: String,
        password_hash: String,
    },
    Updated {
        name: String,
        email: String,
    },
    PasswordUpdated {
        password_hash: String,
    },
}

impl StructAggregateImpl for User {

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        let event_type = event.event_type.as_str();
        println!("Applying event type: {}", event_type);
        let data: UserRequests = event.deserialize()?;

        match data {
            UserRequests::Created { name, email, password_hash } => {
                self.name = name;
                self.email = email;
                self.password_hash = password_hash;
            },
            UserRequests::Updated { name, email } => {
                self.name = name;
                self.email = email;
            },
            UserRequests::PasswordUpdated { password_hash } => {
                self.password_hash = password_hash;
            },
        };
        
        Ok(())
    }

    fn get_type(&self) -> &str {
        "user"
    }

}

impl CanRequest<UserRequests, UserRequests> for User {
    fn request(&self, request: UserRequests) -> Result<(String, UserRequests), EventStoreError> 
    {
        match request {
            UserRequests::Created { name, email, password_hash } => {
                let event = UserRequests::Created { name, email, password_hash };
                Ok(("user_created".to_string(), event))
            },
            UserRequests::Updated { name, email } => {
                let event = UserRequests::Updated { name, email };
                Ok(("user_updated".to_string(), event))
            },
            UserRequests::PasswordUpdated { password_hash } => {
                let event = UserRequests::PasswordUpdated { password_hash };
                Ok(("password_updated".to_string(), event))
            },
        }

    }
}

pub(crate) async fn user_example(event_store: Arc<EventStore>) {

    let context = event_store.clone().get_context();
    let mut user = StructBackedAggregate::<User>::new(context.clone()).await.unwrap();
    let id = user.get_id();
    println!("User Id: {}", id);
    user.request(UserRequests::Created {
        name: "John Doe".to_string(),
        email: "jdoe@example.com".to_string(),
        password_hash: "123456".to_string(),
    }).unwrap();
    context.commit().await.unwrap();


    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);

    let context = event_store.clone().get_context();
    let mut user = StructBackedAggregate::<User>::load( context.clone(), id).await.unwrap();
    user.request(UserRequests::Updated {
        name: "Samuel Jackson".to_string(),
        email: "sammyj@example.com".to_string(),
    }).unwrap();
    context.commit().await.unwrap();
    
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);


    let context = event_store.clone().get_context();
    let user = StructBackedAggregate::<User>::load( context.clone(), id).await.unwrap();
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);
}


