use std::time::{Duration, Instant};

use redis::{aio::ConnectionManager, RedisResult};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot::Sender};
use tracing::{trace, warn};

use crate::redis_cmd::{delete_lock_cmd, load_delete_script};

use super::{DeleteResult, LockId, LockName};

pub type Request = (LockName, LockId, Option<Sender<DeleteResult>>);
pub async fn run(
    mut con: ConnectionManager,
    mut delete_rx: UnboundedReceiver<Request>,
    min_request_period: Duration,
) {
    // Loading the lock-delete script to redis
    if let Err(error) = load_delete_script(&mut con).await {
        warn!(?error, "failed loading script for deleting locks");
    }

    let mut latest_request = Instant::now();
    let mut queue = Vec::new();
    loop {
        // Receiving all enqued tasks
        match delete_rx.recv().await {
            Some(e) => queue.push(e),
            None => return, // signal to stop thread
        }
        while let Ok(e) = delete_rx.try_recv() {
            queue.push(e);
        }

        // Sleeping if necessary to not overwhelm redis
        let el = latest_request.elapsed();
        if let Some(duration) = min_request_period.checked_sub(el) {
            trace!(?duration, "delete locks thread is sleeping");
            tokio::time::sleep(duration).await;
        }

        // Batching lock extension in a pipeline
        let mut pipe = redis::pipe();
        for (name, id, _) in &queue {
            pipe.add_command(delete_lock_cmd(name.clone(), *id));
        }

        // Sending pipeline to redis
        latest_request = Instant::now();
        let result: RedisResult<Vec<u8>> = pipe.query_async(&mut con).await;
        match result {
            Ok(del) => {
                assert_eq!(del.len(), queue.len());
                for (code, (_, _, tx)) in del.into_iter().zip(queue.drain(..)) {
                    if let Some(tx) = tx {
                        if code == 0 {
                            let _ = tx.send(DeleteResult::Deleted);
                        } else {
                            let _ = tx.send(DeleteResult::Expired);
                        }
                    }
                }
            }
            Err(error) => {
                if error.kind() == redis::ErrorKind::NoScriptError {
                    if let Err(error) = load_delete_script(&mut con).await {
                        warn!(?error, "failed loading script for deleting locks");
                    }
                } else {
                    warn!(?error, "failed delete locks")
                }
            }
        }
    }
}
