# redlock_batched - Efficient Redlock management
This implementation of [Redlock](http://redis.io/topics/distlock) 
processes creating, updating and deleting `RedLock`s in batches (redis pipeline).  
Due to batching the number of operations per second is reduced and thousands of `RedLock`s 
can be handled concurrently.

# Features
- Creating, updating the TTL (time to  live) and deleting `RedLock` in batches
- Creating, updating and deleting a single `RedLock`
- Tested with `redis` and `kvrocks`

# Usage
The following program expects redis to run with an open port at `6379`  
```rust
use std::time::Duration;

use redis::AsyncCommands;
use redlock_batched::{LockManager, RedisLock};

#[tokio::main]
async fn main() {
    let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
    let mut con = redis::aio::ConnectionManager::new(client).await.unwrap();
    let manager = LockManager::build(con.clone()).finalize();

    // Creating a lock
    let lock = RedisLock::new("my_lock".to_string(), Duration::from_secs(5));
    let rx = manager.create_lock(&lock);
    match rx.await.unwrap() {
        redlock_batched::CreationResult::Ready => println!("lock has been created"),
        redlock_batched::CreationResult::Canceled => unreachable!(),
    };
    let value: String = con.get(&lock.name).await.unwrap();
    assert_eq!(value, lock.lock_id.to_string());

    // Updating the time to live of the lock
    
    let rx = manager.update_lock_ttl_with_result(&lock.clone().with_ttl(Duration::from_secs(1)));
    match rx.await.unwrap() {
        redlock_batched::UpdateResult::Updated => println!("TTL of lock has been updated"),
        redlock_batched::UpdateResult::Expired => unreachable!(),
    }

    // Deleting the lock
    let rx = manager.delete_lock_with_result(lock.name.clone(), lock.lock_id);
    match rx.await.unwrap() {
        redlock_batched::DeleteResult::Deleted => println!("the lock has been deleted"),
        redlock_batched::DeleteResult::Expired => unreachable!(),
    }
    let value: Option<String> = con.get(&lock.name).await.unwrap();
    assert_eq!(value, None);
}

```
Have a look at `tests/integration_test.rs` for more code examples