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
        redlock_batched::DeleteResult::Deleted => println!("the lock has been delete"),
        redlock_batched::DeleteResult::Expired => unreachable!(),
    }
    let value: Option<String> = con.get(&lock.name).await.unwrap();
    assert_eq!(value, None);
}
