# evercore_sqlx 
This crate provides an sqlx pool based storage engine for evercore.

## Running tests

The `docker-compose` file contains the settings for mysql and postgres to test.
This should be started before executing the tests:

```
docker-compose up
```

Currently, the tests require single-threaded due to some behavior with `#[tokio-test]`
as well as sqlite.  

```
cargo test -- --test-threads=1
```

