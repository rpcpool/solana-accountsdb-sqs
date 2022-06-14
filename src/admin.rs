use {
    crate::config::{ConfigFilters, ConfigTransactionsFilter},
    futures::{
        future::BoxFuture,
        stream::{Stream, StreamExt},
    },
    log::*,
    rand::{distributions::Alphanumeric, thread_rng, Rng},
    redis::{aio::Connection, AsyncCommands, Client as RedisClient, Pipeline, RedisError, Value},
    serde::Deserialize,
    std::{iter, pin::Pin},
    thiserror::Error,
    tokio::time::{sleep, Duration},
};

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("deserialize error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type AdminResult<T = ()> = Result<T, AdminError>;

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct ConfigMgmt {
    redis: RedisClient,
    lock_key: String,
    #[derivative(Debug = "ignore")]
    pub pubsub: Pin<Box<dyn Stream<Item = ConfigMgmtMsg> + Send + Sync>>,
}

impl ConfigMgmt {
    pub async fn new(redis: RedisClient, channel: &str, lock_key: String) -> AdminResult<Self> {
        let mut pubsub = redis.get_async_connection().await?.into_pubsub();
        pubsub.subscribe(channel).await?;

        Ok(Self {
            redis,
            lock_key,
            pubsub: Box::pin(pubsub.into_on_message().filter_map(|msg| async move {
                match serde_json::from_slice(msg.get_payload_bytes()) {
                    Ok(msg) => Some(msg),
                    Err(error) => {
                        error!("failed to decode config management message: {:?}", error);
                        None
                    }
                }
            })),
        })
    }

    fn get_lock_token() -> String {
        let mut rng = thread_rng();
        iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(128)
            .collect()
    }

    async fn read_with_lock_key<T, F>(&self, lock_time: Duration, f: F) -> AdminResult<T>
    where
        F: for<'a> Fn(&'a mut Connection) -> BoxFuture<'a, AdminResult<T>>,
    {
        const UNSET_SCRIPT: &str = r#"
if redis.call("get", KEYS[1]) == ARGV[1]
then
    return redis.call("del", KEYS[1])
else
    return 0
end"#;

        let token = Self::get_lock_token();
        let mut connection = self.redis.get_async_connection().await?;
        loop {
            let mut lock_cmd = redis::cmd("SET");
            lock_cmd.arg(&self.lock_key).arg(&token);
            lock_cmd.arg("PX").arg(lock_time.as_millis() as usize);
            match lock_cmd.arg("NX").query_async(&mut connection).await? {
                Value::Nil => {
                    sleep(Duration::from_micros(1_000)).await;
                    continue;
                }
                Value::Okay => {}
                _ => unreachable!(),
            }

            let result = f(&mut connection).await;

            let mut eval_cmd = redis::cmd("EVAL");
            eval_cmd.arg(UNSET_SCRIPT);
            eval_cmd.arg(1).arg(&self.lock_key).arg(&token);
            match eval_cmd.query_async(&mut connection).await? {
                0 => {} // lock key was overwritten
                1 => break result,
                _ => unreachable!(),
            }
        }
    }

    pub async fn write_with_lock_key<F>(&self, f: F) -> AdminResult
    where
        F: Fn(&mut Pipeline),
    {
        let mut connection = self.redis.get_async_connection().await?;
        loop {
            redis::cmd("WATCH")
                .arg(&self.lock_key)
                .query_async(&mut connection)
                .await?;

            let mut pipe = redis::pipe();
            f(pipe.atomic());

            let result: Value = pipe.query_async(&mut connection).await?;
            match result {
                Value::Nil => {
                    redis::cmd("UNWATCH").query_async(&mut connection).await?;
                    continue;
                }
                _ => break,
            }
        }

        Ok(())
    }

    pub async fn get_global_config(&self, config_key: String) -> AdminResult<ConfigFilters> {
        let lock_time = Duration::from_secs(1);
        self.read_with_lock_key(lock_time, |connection| {
            let config_key = config_key.clone();
            Box::pin(async move {
                let data: String = connection.get(config_key).await?;
                Ok(ConfigFilters {
                    admin: None,
                    ..serde_json::from_str(&data)?
                })
            })
        })
        .await
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filter_type", rename_all = "snake_case")]
pub enum ConfigMgmtMsg {
    Transactions(ConfigMgmtMsgTransactions),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "filter_action", rename_all = "snake_case")]
pub enum ConfigMgmtMsgTransactions {
    Add {
        name: String,
        config: ConfigTransactionsFilter,
    },
    Remove {
        name: String,
    },
}
