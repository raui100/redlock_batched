use std::time::Duration;

use uuid::Uuid;

pub mod create;
pub mod delete;
pub mod manager;
pub mod update;

pub type LockName = String;
pub type LockId = Uuid;
pub type CancelId = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CreationResult {
    Ready,
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UpdateResult {
    Updated,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeleteResult {
    Deleted,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedisLock {
    /// Value of the key that is used for locking (eg: "/my/redis/lock")
    pub name: LockName,
    /// Time to live of the lock (eg: 500 ms)
    pub ttl: Duration,
    /// Random value to uniquely identify a lock (eg: "E1AD3B90-D875-4666-9206-EEAE491BBD5D")
    pub lock_id: LockId,
    /// Requested locks that are waiting for their turn can be canceled
    pub cancel_id: Option<CancelId>,
}

impl RedisLock {
    pub fn new(name: String, ttl: Duration) -> Self {
        Self {
            name,
            ttl,
            lock_id: Uuid::new_v4(),
            cancel_id: None,
        }
    }

    /// Setting the `cancel_id` (eg. SHA256 hash)
    pub fn with_cancel_id(self, cancel_id: CancelId) -> Self {
        Self {
            cancel_id: Some(cancel_id),
            ..self
        }
    }
}
