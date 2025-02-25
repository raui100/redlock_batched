use std::time::{Duration, Instant};

use redis::{aio::ConnectionManager, RedisResult};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};
use tracing::{trace, warn};

use crate::redis_cmd::{extend_lock_cmd, load_extend_script};

use super::{RedisLock, UpdateResult};

pub type Request = (RedisLock, Option<Sender<UpdateResult>>);
pub async fn run(
    mut con: ConnectionManager,
    mut extend_rx: UnboundedReceiver<Request>,
    min_request_period: Duration,
) {
    // Loading the lock-extend script to redis
    if let Err(error) = load_extend_script(&mut con).await {
        warn!(?error, "failed loading script for deleting locks");
    }
    let mut latest_request = Instant::now();
    let mut queue = Vec::new();
    loop {
        // Receiving all enqued tasks
        match extend_rx.recv().await {
            Some(e) => queue.push(e),
            None => return, // signal to stop thread
        }
        while let Ok(e) = extend_rx.try_recv() {
            queue.push(e);
        }

        // Sleeping if necessary to not overwhelm redis
        let el = latest_request.elapsed();
        if let Some(duration) = min_request_period.checked_sub(el) {
            trace!(?duration, "extending locks thread is sleeping");
            tokio::time::sleep(duration).await;
        }

        // Batching lock extension in a pipeline
        let mut pipe = redis::pipe();
        for (lock, _) in &queue {
            pipe.add_command(extend_lock_cmd(lock.name.clone(), lock.lock_id, lock.ttl));
        }

        // Sending pipeline to redis
        latest_request = Instant::now();
        let result: RedisResult<Vec<u8>> = pipe.query_async(&mut con).await;
        match result {
            Ok(del) => {
                assert_eq!(del.len(), queue.len());
                for (code, (_, tx)) in del.into_iter().zip(queue.drain(..)) {
                    if let Some(tx) = tx {
                        if code == 0 {
                            let _ = tx.send(UpdateResult::Updated);
                        } else {
                            let _ = tx.send(UpdateResult::Expired);
                        }
                    }
                }
            }
            Err(error) => {
                if error.kind() == redis::ErrorKind::NoScriptError {
                    if let Err(error) = load_extend_script(&mut con).await {
                        warn!(?error, "failed loading script for deleting locks");
                    }
                } else {
                    warn!(?error, "failed extending locks")
                }
            }
        }
    }
}
