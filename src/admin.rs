use {
    crate::{
        config::{ConfigFilters, ConfigRedis, PubkeyWithSource, PubkeyWithSourceError},
        metrics::health::{set_health, HealthInfoType},
    },
    futures::stream::{Stream, StreamExt},
    log::*,
    redis::{
        aio::Connection, AsyncCommands, Client as RedisClient, RedisError, RedisResult, Value,
    },
    serde::{Deserialize, Serialize},
    solana_sdk::pubkey::Pubkey,
    std::{fmt, future::Future, ops::Deref, pin::Pin},
    thiserror::Error,
    tokio::{
        sync::oneshot,
        time::{error::Elapsed as ElapsedError, sleep, timeout, Duration},
    },
};

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("redis timeout: {0}")]
    RedisTimeout(#[from] ElapsedError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pubkeys failed with redis: {0}")]
    Pubkeys(#[from] PubkeyWithSourceError),
}

pub type AdminResult<T = ()> = Result<T, AdminError>;

pub struct ConfigMgmt {
    pub config: ConfigRedis,
    shutdown: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for ConfigMgmt {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ConfigMgmt")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ConfigMgmt {
    pub async fn new(config: ConfigRedis, node: Option<String>) -> AdminResult<Self> {
        let shutdown = match node {
            Some(node) => {
                let (send, recv) = oneshot::channel();
                tokio::spawn(Self::heartbeat_loop(
                    config.url.deref().clone(),
                    recv,
                    config.channel.clone(),
                    node,
                ));
                Some(send)
            }
            None => None,
        };

        Ok(Self { config, shutdown })
    }

    pub async fn with_timeout<F, T>(future: F) -> AdminResult<T>
    where
        F: Future<Output = RedisResult<T>>,
    {
        Ok(timeout(Duration::from_secs(10), future).await??)
    }

    pub async fn get_pubsub(
        &self,
    ) -> AdminResult<Pin<Box<dyn Stream<Item = ConfigMgmtMsg> + Send + Sync>>> {
        let connection = Self::with_timeout(self.config.url.get_async_connection()).await?;

        let mut pubsub = connection.into_pubsub();
        Self::with_timeout(pubsub.subscribe(&self.config.channel)).await?;

        Ok(Box::pin(pubsub.into_on_message().filter_map(
            |msg| async move {
                match msg.get_payload::<Value>() {
                    Ok(msg) => debug!("received admin message: {:?}", msg),
                    Err(error) => error!("failed to decode admin message: {}", error),
                }

                match serde_json::from_slice(msg.get_payload_bytes()) {
                    Ok(msg) => Some(msg),
                    Err(error) => {
                        error!("failed to decode config management message: {:?}", error);
                        None
                    }
                }
            },
        )))
    }

    pub fn shutdown(self) {
        if let Some(shutdown) = self.shutdown {
            let _ = shutdown.send(());
        }
    }

    pub async fn get_global_config(&self) -> AdminResult<ConfigFilters> {
        let mut connection = Self::with_timeout(self.config.url.get_async_connection()).await?;
        let data: String = Self::with_timeout(connection.get(&self.config.config)).await?;
        let mut config: ConfigFilters = serde_json::from_str(&data)?;
        config.load_pubkeys(&mut connection).await?;
        Ok(config)
    }

    pub async fn set_global_config(&self, config: &ConfigFilters) -> AdminResult {
        let mut connection = Self::with_timeout(self.config.url.get_async_connection()).await?;
        let mut pipe = redis::pipe();
        config.save_pubkeys(&mut pipe)?;
        pipe.set(&self.config.config, serde_json::to_string(config)?);
        Self::with_timeout(pipe.query_async::<_, ()>(&mut connection)).await?;
        Ok(())
    }

    pub async fn send_message(&self, message: &ConfigMgmtMsg) -> AdminResult<usize> {
        let mut connection = Self::with_timeout(self.config.url.get_async_connection()).await?;
        Self::send_message2(&mut connection, &self.config.channel, message).await
    }

    async fn send_message2(
        connection: &mut Connection,
        channel: &str,
        message: &ConfigMgmtMsg,
    ) -> AdminResult<usize> {
        Self::with_timeout(connection.publish(channel, serde_json::to_string(message)?)).await
    }

    async fn heartbeat_loop(
        url: RedisClient,
        mut shutdown: oneshot::Receiver<()>,
        channel: String,
        node: String,
    ) {
        let mut message = ConfigMgmtMsg::Request {
            node: Some(node),
            id: 0,
            action: ConfigMgmtMsgRequest::Heartbeat,
        };

        loop {
            let mut connection = match Self::with_timeout(url.get_async_connection()).await {
                Ok(connection) => {
                    set_health(HealthInfoType::RedisHeartbeat, Ok(()));
                    info!("created connection for heartbeat");
                    connection
                }
                Err(error) => {
                    set_health(HealthInfoType::RedisHeartbeat, Err(()));
                    error!("failed to setup connection for hearbeat: {:?}", error);
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };

            loop {
                match Self::send_message2(&mut connection, &channel, &message).await {
                    Ok(_) => {
                        set_health(HealthInfoType::RedisHeartbeat, Ok(()));
                        debug!("heartbeat send");
                    }
                    Err(error) => {
                        set_health(HealthInfoType::RedisHeartbeat, Err(()));
                        error!("heartbeat error: {:?}", error);
                        break;
                    }
                }

                if let ConfigMgmtMsg::Request { id, .. } = &mut message {
                    *id = id.wrapping_add(1);
                }

                tokio::select! {
                    _ = sleep(Duration::from_secs(10)) => {},
                    _ = &mut shutdown => {
                        set_health(HealthInfoType::RedisHeartbeat, Err(()));
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum ConfigMgmtMsg {
    Request {
        node: Option<String>,
        id: u64,
        #[serde(flatten)]
        action: ConfigMgmtMsgRequest,
    },
    Response {
        node: String,
        id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum ConfigMgmtMsgRequest {
    Heartbeat,
    Ping,
    Version,
    Global,
    GetConfig,
    PubkeysSet {
        filter: ConfigMgmtMsgFilter,
        action: ConfigMgmtMsgAction,
        #[serde(
            deserialize_with = "PubkeyWithSource::deserialize_pubkey",
            serialize_with = "PubkeyWithSource::serialize_pubkey"
        )]
        pubkey: Pubkey,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMgmtMsgFilter {
    Accounts {
        name: String,
        kind: ConfigMgmtMsgFilterAccounts,
    },
    Transactions {
        name: String,
        kind: ConfigMgmtMsgFilterTransactions,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMgmtMsgFilterAccounts {
    Account,
    Owner,
    TokenkegOwner,
    TokenkegDelegate,
}

impl ConfigMgmtMsgFilterAccounts {
    pub const fn as_str(&self) -> &str {
        match *self {
            Self::Account => "account",
            Self::Owner => "owner",
            Self::TokenkegOwner => "tokenkeg_owner",
            Self::TokenkegDelegate => "tokenkeg_delegate",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMgmtMsgFilterTransactions {
    AccountsInclude,
    AccountsExclude,
}

impl ConfigMgmtMsgFilterTransactions {
    pub const fn as_str(&self) -> &str {
        match *self {
            Self::AccountsInclude => "accounts.include",
            Self::AccountsExclude => "accounts.exclude",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigMgmtMsgAction {
    Add,
    Remove,
}
