use eventide::{EventStore, aggregate::StructBackedAggregate};

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
struct User {
    name: String,
    email: String,
    password_hash: String,
}


#[derive(Serialize, Deserialize, Debug)]
enum UserRequests {
    UserCreated {
        name: String,
        email: String,
        password_hash: String,
    },
    UserUpdated {
        name: String,
        email: String,
    },
    UserPasswordUpdated {
        password_hash: String,
    },
}

impl StructAggregateImpl for User {

    fn apply_event(&mut self, event: &Event) -> Result<(), EventStoreError> {
        let event_type = event.event_type.as_str();
        println!("Applying event type: {}", event_type);
        let data: UserRequests = event.deserialize()?;

        match data {
            UserRequests::UserCreated { name, email, password_hash } => {
                self.name = name;
                self.email = email;
                self.password_hash = password_hash;
            },
            UserRequests::UserUpdated { name, email } => {
                self.name = name;
                self.email = email;
            },
            UserRequests::UserPasswordUpdated { password_hash } => {
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
            UserRequests::UserCreated { name, email, password_hash } => {
                let event = UserRequests::UserCreated { name, email, password_hash };
                Ok(("user_created".to_string(), event))
            },
            UserRequests::UserUpdated { name, email } => {
                let event = UserRequests::UserUpdated { name, email };
                Ok(("user_updated".to_string(), event))
            },
            UserRequests::UserPasswordUpdated { password_hash } => {
                let event = UserRequests::UserPasswordUpdated { password_hash };
                Ok(("password_updated".to_string(), event))
            },
        }

    }
}

pub(crate) async fn user_example(event_store: &EventStore) {

    let context = event_store.get_context();
    let mut user = StructBackedAggregate::<User>::new(context.clone()).await.unwrap();
    let id = user.get_id();
    println!("User Id: {}", id);
    user.reqeust(UserRequests::UserCreated {
        name: "John Doe".to_string(),
        email: "jdoe@example.com".to_string(),
        password_hash: "123456".to_string(),
    }).unwrap();
    context.commit().await.unwrap();


    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);

    let context = event_store.get_context();
    let mut user = StructBackedAggregate::<User>::load( context.clone(), id).await.unwrap();
    user.reqeust(UserRequests::UserUpdated {
        name: "Samuel Jackson".to_string(),
        email: "sammyj@example.com".to_string(),
    }).unwrap();
    context.commit().await.unwrap();
    
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);


    let context = event_store.get_context();
    let user = StructBackedAggregate::<User>::load( context.clone(), id).await.unwrap();
    let user_state = user.owned_state();
    println!("User State: {:?}", user_state);

    let create_command = UserRequests::UserCreated {
        name: "John Doe".to_string(),
        email: "jdoe@example.com".to_string(),
        password_hash: "123456".to_string(),
    };

}


