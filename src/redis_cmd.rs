use redis::{aio::ConnectionLike, cmd, Cmd, RedisResult, Script};
use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};
use tracing::warn;
use uuid::Uuid;

use crate::{locks::UpdateResult, RedisLock};

pub const DELETE_LOCK_LUA: &str = r#"if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 0
else
    return 1
end"#;

pub const EXTEND_LOCK_LUA: &str = r#"if redis.call("get", KEYS[1]) ~= ARGV[1] then
  return 1
else
  if redis.call("set", KEYS[1], ARGV[1], ARGV[2], ARGV[3]) ~= nil then
    return 0
  else
    return 1
  end
end
"#;

pub static DELETE_LOCK_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(DELETE_LOCK_LUA));
pub static EXTEND_LOCK_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(EXTEND_LOCK_LUA));

pub fn create_lock_cmd(lock: &RedisLock) -> Cmd {
    let (expiration, ttl) = TimeToLive::from(lock.ttl).command();
    let mut command = cmd("SET");
    command
        .arg(&lock.name)
        .arg(lock.lock_id.to_string())
        .arg("NX")
        .arg(expiration)
        .arg(ttl);
    command
}

pub fn delete_lock_cmd(name: String, id: Uuid) -> Cmd {
    let mut command = cmd("EVALSHA");
    command
        .arg(DELETE_LOCK_SCRIPT.get_hash())
        .arg(1)
        .arg(name)
        .arg(id.to_string());
    command
}

pub fn extend_lock_cmd(name: String, id: Uuid, ttl: Duration) -> Cmd {
    let (expiration, duration) = TimeToLive::from(ttl).command();
    let mut command = cmd("EVALSHA");
    command
        .arg(EXTEND_LOCK_SCRIPT.get_hash())
        .arg(1)
        .arg(name)
        .arg(id.to_string())
        .arg(expiration)
        .arg(duration);
    command
}

/// Loading the script for extending locks to redis
pub async fn load_extend_script(con: &mut impl ConnectionLike) -> RedisResult<String> {
    EXTEND_LOCK_SCRIPT.prepare_invoke().load_async(con).await
}

/// Loading the script for deleting locks to redis
pub async fn load_delete_script(con: &mut impl ConnectionLike) -> RedisResult<String> {
    DELETE_LOCK_SCRIPT.prepare_invoke().load_async(con).await
}

/// Creating a single lock. Retries until successfull
pub async fn create_lock(
    con: &mut impl ConnectionLike,
    redis_lock: RedisLock,
    retry_delay: Duration,
) -> RedisResult<()> {
    loop {
        let latest_retry = Instant::now();
        match create_lock_cmd(&redis_lock)
            .query_async::<Option<()>>(con)
            .await?
        {
            Some(_) => return Ok(()),
            None => tokio::time::sleep(retry_delay.saturating_sub(latest_retry.elapsed())).await,
        }
    }
}

/// Deleting a single lock
pub async fn delete_lock(
    con: &mut impl ConnectionLike,
    lock_name: String,
    lock_id: Uuid,
) -> RedisResult<UpdateResult> {
    DELETE_LOCK_SCRIPT
        .prepare_invoke()
        .key(lock_name)
        .arg(lock_id.to_string())
        .invoke_async(con)
        .await
        .map(|n: u8| {
            if n == 0 {
                UpdateResult::Expired
            } else {
                UpdateResult::Updated
            }
        })
}

/// Extending a single lock
pub async fn extend_lock(
    con: &mut impl ConnectionLike,
    lock_name: String,
    lock_id: Uuid,
    ttl: Duration,
) -> RedisResult<UpdateResult> {
    let (expiration, duration) = TimeToLive::from(ttl).command();
    EXTEND_LOCK_SCRIPT
        .prepare_invoke()
        .key(lock_name)
        .arg(lock_id.to_string())
        .arg(expiration)
        .arg(duration)
        .invoke_async(con)
        .await
        .map(|n: u8| {
            if n == 0 {
                UpdateResult::Expired
            } else {
                UpdateResult::Updated
            }
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeToLive {
    Seconds(u64),
    MilliSeconds(u64),
}

impl TimeToLive {
    pub fn command(self) -> (&'static str, u64) {
        match self {
            TimeToLive::Seconds(n) => ("EX", n),
            TimeToLive::MilliSeconds(n) => ("PX", n),
        }
    }
}

impl From<Duration> for TimeToLive {
    fn from(value: Duration) -> Self {
        if let Ok(mut ms) = u64::try_from(value.as_millis()) {
            if ms == 0 {
                warn!("the time to live of the lock must be >= 1 ms");
                ms = 1;
            }
            Self::MilliSeconds(ms)
        } else {
            Self::Seconds(value.as_secs())
        }
    }
}
