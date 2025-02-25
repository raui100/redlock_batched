use std::{sync::Arc, time::Duration};

use redis::{aio::ConnectionManager, AsyncCommands};
use redlock_batched::{
    redis_cmd::{create_lock, delete_lock, extend_lock},
    CreationResult, DeleteResult, LockManager, RedisLock, UpdateResult,
};
use testcontainers::{core::WaitFor, runners::AsyncRunner, ContainerAsync, Image, ImageExt};
use testcontainers_modules::redis::Redis;
use tracing::debug;
use uuid::Uuid;

#[tokio::test]
async fn integration_test() {
    tracing_subscriber::fmt().init();
    let redis = tokio::spawn(async move {
        let (container, con) = run_redis().await;
        integration_test_inner(container, con).await;
    });
    let kvrocks = tokio::spawn(async move {
        let (container, con) = run_kvrocks().await;
        integration_test_inner(container, con).await;
    });
    redis.await.unwrap();
    kvrocks.await.unwrap();
}

async fn integration_test_inner(container: ContainerAsync<impl Image>, mut con: ConnectionManager) {
    debug!("Creating a lock");
    let lock = RedisLock::new("test1".into(), Duration::from_secs(5));
    create_lock(&mut con, lock.clone(), Duration::from_secs(1))
        .await
        .unwrap();
    let value: String = con.get(&lock.name).await.unwrap();
    assert_eq!(value, lock.lock_id.to_string());

    debug!("Extending the TTL of the lock");
    let ttl: u32 = con.ttl(&lock.name).await.unwrap();
    extend_lock(
        &mut con,
        lock.name.clone(),
        lock.lock_id,
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let new_ttl: u32 = con.ttl(&lock.name).await.unwrap();
    assert!(ttl < new_ttl);

    debug!("Deleting the lock");
    delete_lock(&mut con, lock.name.clone(), lock.lock_id)
        .await
        .unwrap();
    let value: Option<String> = con.get(&lock.name).await.unwrap();
    assert_eq!(value, None);

    // Testing the lock manager
    let manager = LockManager::build(con).finalize();

    // Creating a lock
    let create = manager.create_lock(&lock).await.unwrap();
    assert_eq!(create, CreationResult::Ready);

    // Creating the same lock enques the lock
    let mut request = manager.create_lock(&lock.clone().with_cancel_id([0; 32]));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(request.try_recv().is_err());

    // Canceling the lock
    manager.cancel_lock([0; 32]);
    let y2 = tokio::time::timeout(Duration::from_millis(500), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(y2, CreationResult::Canceled);

    // Setting the TTL to zero for the current lock
    let result = manager
        .update_lock_ttl_with_result(&RedisLock {
            ttl: Duration::from_millis(1),
            ..lock.clone()
        })
        .await
        .unwrap();
    assert_eq!(result, UpdateResult::Updated);

    // Deleting the current lock fails because it is already expired
    tokio::time::sleep(Duration::from_millis(100)).await; // waiting a little for redis to expire the lock
    let ext = manager.update_lock_ttl_with_result(&lock).await.unwrap();
    let del = manager
        .delete_lock_with_result(lock.name, lock.lock_id)
        .await
        .unwrap();
    assert_eq!(ext, UpdateResult::Expired);
    assert_eq!(del, DeleteResult::Expired);

    debug!("Creating 1000 locks");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..1000 {
        let lock = RedisLock::new(Uuid::new_v4().into(), Duration::from_secs(60));
        let rx = manager.create_lock(&lock);
        tasks.spawn(async move { (lock, rx.await) });
    }
    let results = tokio::time::timeout(Duration::from_secs(5), tasks.join_all())
        .await
        .unwrap();

    let results: Vec<_> = results.into_iter().map(|e| (e.0, e.1.unwrap())).collect();
    results
        .iter()
        .for_each(|(_, r)| assert_eq!(r, &CreationResult::Ready));

    debug!("Extending all locks");
    let mut tasks = tokio::task::JoinSet::new();
    for (lock, _) in results {
        let rx = manager.update_lock_ttl_with_result(&lock);
        tasks.spawn(async move { (lock, rx.await) });
    }
    let results = tokio::time::timeout(Duration::from_secs(5), tasks.join_all())
        .await
        .unwrap();
    let results: Vec<_> = results
        .into_iter()
        .map(|e| {
            let result = e.1.unwrap();
            assert_eq!(result, UpdateResult::Updated);
            (e.0, result)
        })
        .collect();

    debug!("Deleting all locks");
    let mut tasks = tokio::task::JoinSet::new();
    for (lock, _) in results.into_iter() {
        let rx = manager.delete_lock_with_result(lock.name.clone(), lock.lock_id);
        tasks.spawn(async move { (lock, rx.await) });
    }
    let results = tokio::time::timeout(Duration::from_secs(5), tasks.join_all())
        .await
        .unwrap();
    let results: Vec<RedisLock> = results
        .into_iter()
        .map(|e| {
            let result = e.1.unwrap();
            assert_eq!(result, DeleteResult::Deleted);
            e.0
        })
        .collect();

    debug!("Creating, extending and deleting locks in fast succession");
    let manager = Arc::new(manager);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..10 {
        for lock in &results {
            tasks.spawn(create_extend_delete(manager.clone(), lock.clone()));
        }
    }
    let _results = tokio::time::timeout(Duration::from_secs(10), tasks.join_all())
        .await
        .unwrap();

    container.stop().await.unwrap()
}

pub async fn create_extend_delete(manager: Arc<LockManager>, lock: RedisLock) {
    let result = manager.create_lock(&lock).await.unwrap();
    assert_eq!(result, CreationResult::Ready);
    let result = manager.update_lock_ttl_with_result(&lock).await.unwrap();
    assert_eq!(result, UpdateResult::Updated);
    let result = manager
        .delete_lock_with_result(lock.name, lock.lock_id)
        .await
        .unwrap();
    assert_eq!(result, DeleteResult::Deleted);
}

pub async fn run_redis() -> (ContainerAsync<Redis>, ConnectionManager) {
    let container = Redis::default()
        .with_tag("latest")
        .start()
        .await
        .expect("starting redis container");

    let host = get_host(&container).await;
    let port = get_port(&container, 6379).await;
    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    let con = redis::aio::ConnectionManager::new(client).await.unwrap();
    (container, con)
}

pub async fn run_kvrocks() -> (ContainerAsync<KvRocks>, ConnectionManager) {
    let container = KvRocks::default()
        .with_tag("latest")
        .start()
        .await
        .expect("starting redis container");

    let host = get_host(&container).await;
    let port = get_port(&container, 6666).await;
    let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
    let con = redis::aio::ConnectionManager::new(client).await.unwrap();
    (container, con)
}

pub async fn get_host<I: Image>(container: &ContainerAsync<I>) -> String {
    container
        .get_host()
        .await
        .expect("used in tests")
        .to_string()
}

pub async fn get_port<I: Image>(container: &ContainerAsync<I>, port: u16) -> u16 {
    container
        .get_host_port_ipv4(port)
        .await
        .expect("used in tests")
}

#[derive(Debug, Default, Clone)]
pub struct KvRocks {
    /// (remove if there is another variable)
    /// Field is included to prevent this struct to be a unit struct.
    /// This allows extending functionality (and thus further variables) without breaking changes
    _priv: (),
}

impl Image for KvRocks {
    fn name(&self) -> &str {
        "apache/kvrocks"
    }

    fn tag(&self) -> &str {
        "2.11.1"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stdout("Ready to accept connections")]
    }
}
