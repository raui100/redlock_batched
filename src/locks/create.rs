use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use redis::{aio::ConnectionManager, RedisResult};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot::Sender,
};
use tracing::{debug, trace, warn};

use crate::redis_cmd::create_lock_cmd;

use super::{CancelId, CreationResult, LockName, RedisLock};

pub type Request = (RedisLock, Sender<CreationResult>);
pub async fn run(
    mut con: ConnectionManager,
    mut create_rx: UnboundedReceiver<Request>,
    delete_tx: UnboundedSender<super::delete::Request>,
    mut cancel_rx: UnboundedReceiver<CancelId>,
    min_request_period: Duration,
    cleanup_period: Duration,
) {
    let mut latest_request = Instant::now();
    let mut latest_cleanup = Instant::now();
    let mut queue =
        HashMap::<LockName, VecDeque<(RedisLock, Option<Sender<CreationResult>>)>>::new();

    loop {
        // Receiving all enqued tasks
        loop {
            match create_rx.try_recv() {
                Ok((lock, tx)) => queue
                    .entry(lock.name.clone())
                    .or_default()
                    .push_back((lock, Some(tx))),
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => break,
                    mpsc::error::TryRecvError::Disconnected => return, // signal to stop thread
                },
            }
        }

        // Deleting all tasks associated with a cancel ID
        while let Ok(id) = cancel_rx.try_recv() {
            for v in queue.values_mut() {
                v.retain_mut(|(lock, tx)| {
                    if lock.cancel_id == Some(id) {
                        let _ = tx.take().map(|tx| tx.send(CreationResult::Canceled));
                        false
                    } else {
                        true
                    }
                });
            }
        }

        // Cleaning up to reduce the RAM usage
        if latest_cleanup.elapsed() > cleanup_period {
            latest_cleanup = Instant::now();
            debug!("Cleaning up and decreasing RAM usage");
            queue.retain(|_, v| !v.is_empty()); // no waiting requests
            queue.shrink_to_fit(); // minimizing RAM usage
        }

        // Sleeping if necessary to not overwhelm redis
        let el = latest_request.elapsed();
        if let Some(duration) = min_request_period.checked_sub(el) {
            trace!(?duration, "creating locks thread is sleeping");
            tokio::time::sleep(duration).await;
        }

        // Batching lock creation in a pipeline
        let mut ordered_values = Vec::new();
        let mut pipe = redis::pipe();
        for value in queue.values_mut() {
            if let Some((lock, _)) = value.front() {
                pipe.add_command(create_lock_cmd(lock));
                ordered_values.push(value);
            }
        }

        // Removing request from `queue` where the locks have been acquired
        latest_request = Instant::now();
        if !ordered_values.is_empty() {
            trace!(n = ordered_values.len(), "requesting locks from redis");
            let result: RedisResult<Vec<Option<()>>> = pipe.query_async(&mut con).await;
            match result {
                Ok(locks) => {
                    // Counting the number of locks
                    let number_of_locks = locks.iter().filter_map(|&e| e).count();
                    if number_of_locks > 0 {
                        debug!(number_of_locks, "created new locks")
                    } else {
                        trace!("no new locks have been created")
                    }

                    // Removing the requests where a lock has been created
                    assert_eq!(locks.len(), ordered_values.len());
                    locks
                        .into_iter()
                        .zip(ordered_values)
                        .filter_map(|(l, v)| l.map(|_| v))
                        .for_each(|v| {
                            let (lock, tx) = v.pop_front().expect("exists");
                            let tx = tx.expect("exists");
                            if tx.send(CreationResult::Ready).is_err() {
                                debug!("deleting lock that has just been created because the lock recipient is unreachable");
                                let _ = delete_tx.send((lock.name, lock.lock_id, None));
                            }
                        });
                }
                Err(error) => warn!(?error, "failed acquiring locks"),
            }
        }
    }
}
