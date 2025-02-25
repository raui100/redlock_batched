use std::time::Duration;

use redis::aio::ConnectionManager;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    oneshot::{channel, Receiver, Sender},
};

use super::{CancelId, CreationResult, DeleteResult, LockId, LockName, RedisLock, UpdateResult};

/// Manages creating, extending and deleting locks
///
/// Instead of creating redis locks directly requests to create/extend/delete locks are batched.
/// This is done by running three background threads (create/extend/delete) the `LockManager` handles.
/// This helps when directly locking via redis becomes a bottleneck.
/// Furthermore locks are created in the same order as they have been requested.
/// Enqued requests for creating new locks can be cancelled.
#[derive(Clone)]
pub struct LockManager {
    /// Requesting the creation of new locks
    create: UnboundedSender<(RedisLock, Sender<CreationResult>)>,
    /// Extending the time to live of existing locks
    extend: UnboundedSender<(RedisLock, Option<Sender<UpdateResult>>)>,
    /// Deleting existing locks
    delete: UnboundedSender<(LockName, LockId, Option<Sender<DeleteResult>>)>,
    /// Canceling requests for locks that are waiting for their creation
    cancel: UnboundedSender<CancelId>,
}

pub struct LockManagerBuilder {
    pub con: ConnectionManager,
    pub min_request_period: Option<Duration>,
    pub cleanup_period: Option<Duration>,
}

impl LockManagerBuilder {
    pub fn new(con: ConnectionManager) -> Self {
        Self {
            con,
            min_request_period: None,
            cleanup_period: None,
        }
    }

    /// Limits the rate with which commands are being send to redis (default: 1 kHz)
    ///
    /// When setting `period` to 0 the rate is determined by the duration of the calls to redis that are being
    /// executed sequentally.
    ///
    /// When setting `period` to 1 second all requests for creating, extending and deleting locks are collected and
    /// send as a batch to redis via redis pipelines. This would result in three requests per second.
    /// However setting `period` to a high duration means creating, extending and deleting locks might be slow and unresponsive
    pub fn with_minimum_request_period(self, period: Duration) -> Self {
        Self {
            min_request_period: Some(period),
            ..self
        }
    }

    /// Sets the rate with which the buffer is being cleaned up (default: every 10 minutes)
    ///
    /// The objects stored while enqueing the requests for lock creation use very little RAM.
    /// However it makes sense for long living background threads to clean up every now and then to decrease the RAM usage.
    /// Cleaning up can hold up creating new locks and should be done sparingly for good responsiveness.
    pub fn with_cleanup_period(self, period: Duration) -> Self {
        Self {
            cleanup_period: Some(period),
            ..self
        }
    }

    /// Starts the background threads (create/extend/delete locks)
    pub fn finalize(self) -> LockManager {
        // Creating the Sender/Receiver for communicating with the create/extend/delete threads
        let (create_tx, create_rx) = unbounded_channel();
        let (extend_tx, extend_rx) = unbounded_channel();
        let (delete_tx, delete_rx) = unbounded_channel();
        let (cancel_tx, cancel_rx) = unbounded_channel();

        // Setting the default values if neccessary
        let min_request_period = self.min_request_period.unwrap_or(Duration::from_millis(1)); // 1 kHz
        let cleanup_period = self.cleanup_period.unwrap_or(Duration::from_secs(600)); // 10 minutes

        // Running the create/cancel thread in the background
        tokio::spawn(super::create::run(
            self.con.clone(),
            create_rx,
            delete_tx.clone(),
            cancel_rx,
            min_request_period,
            cleanup_period,
        ));

        // Running the extend thread in the background
        tokio::spawn(super::update::run(
            self.con.clone(),
            extend_rx,
            min_request_period,
        ));

        // Running the delete thread in the background
        tokio::spawn(super::delete::run(self.con, delete_rx, min_request_period));

        LockManager {
            create: create_tx,
            extend: extend_tx,
            delete: delete_tx,
            cancel: cancel_tx,
        }
    }
}

const MSG: &str = "The corresponding receiver lives at least as long as the `LockManager`";

impl LockManager {
    pub fn build(con: ConnectionManager) -> LockManagerBuilder {
        LockManagerBuilder::new(con)
    }

    /// Creates a new request for lock creation
    pub fn create_lock(&self, lock: &RedisLock) -> Receiver<CreationResult> {
        let (tx, rx) = channel();
        self.create.send((lock.clone(), tx)).expect(MSG);
        rx
    }

    /// Cancels all requests for lock creation
    pub fn cancel_lock(&self, id: CancelId) {
        self.cancel.send(id).expect(MSG);
    }

    /// Extends the time to live of an existing lock.
    pub fn extend_lock(&self, lock: &RedisLock) {
        self.extend.send((lock.clone(), None)).expect(MSG);
    }

    /// Extends the time to live of an existing lock.
    pub fn extend_lock_with_result(&self, lock: &RedisLock) -> Receiver<UpdateResult> {
        let (tx, rx) = channel();
        self.extend.send((lock.clone(), Some(tx))).expect(MSG);
        rx
    }

    /// Deletes an existing lock
    pub fn delete_lock(&self, name: LockName, id: LockId) {
        self.delete.send((name, id, None)).expect(MSG);
    }

    /// Deletes an existing lock
    pub fn delete_lock_with_result(&self, name: LockName, id: LockId) -> Receiver<DeleteResult> {
        let (tx, rx) = channel();
        self.delete.send((name, id, Some(tx))).expect(MSG);
        rx
    }
}
