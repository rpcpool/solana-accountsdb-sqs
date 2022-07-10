use {
    crate::{
        config::{ConfigFilters, ConfigRedis, PubkeyWithSource, PubkeyWithSourceError},
        prom::health::{set_heath, HealthInfoType},
    },
    futures::stream::{Stream, StreamExt},
    log::*,
    redis::{aio::Connection, AsyncCommands, RedisError},
    serde::{Deserialize, Serialize},
    solana_sdk::pubkey::Pubkey,
    std::{fmt, pin::Pin},
    thiserror::Error,
    tokio::{
        sync::{oneshot, Mutex},
        time,
    },
};

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("pubkeys failed with redis: {0}")]
    Pubkeys(#[from] PubkeyWithSourceError),
}

pub type AdminResult<T = ()> = Result<T, AdminError>;

pub struct ConfigMgmt {
    pub config: ConfigRedis,
    pub pubsub: Pin<Box<dyn Stream<Item = ConfigMgmtMsg> + Send + Sync>>,
    connection: Mutex<Connection>,
    shutdown: oneshot::Sender<()>,
}

impl fmt::Debug for ConfigMgmt {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ConfigMgmt")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ConfigMgmt {
    pub async fn new(config: ConfigRedis, node: String) -> AdminResult<Self> {
        let mut pubsub = config.url.get_async_connection().await?.into_pubsub();
        pubsub.subscribe(&config.channel).await?;

        let connection = config.url.get_async_connection().await?;

        let (send, recv) = oneshot::channel();
        let connection2 = config.url.get_async_connection().await?;
        tokio::spawn(Self::heartbeat_loop(
            connection2,
            recv,
            config.channel.clone(),
            node,
        ));

        Ok(Self {
            config,
            pubsub: Box::pin(pubsub.into_on_message().filter_map(|msg| async move {
                match serde_json::from_slice(msg.get_payload_bytes()) {
                    Ok(msg) => Some(msg),
                    Err(error) => {
                        error!("failed to decode config management message: {:?}", error);
                        None
                    }
                }
            })),
            connection: Mutex::new(connection),
            shutdown: send,
        })
    }

    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }

    pub async fn get_global_config(&self) -> AdminResult<ConfigFilters> {
        let mut connection = self.config.url.get_async_connection().await?;
        let data: String = connection.get(&self.config.config).await?;
        let mut config: ConfigFilters = serde_json::from_str(&data)?;
        config.load_pubkeys(&mut connection).await?;
        Ok(config)
    }

    pub async fn set_global_config(&self, config: &ConfigFilters) -> AdminResult {
        let mut connection = self.config.url.get_async_connection().await?;
        let mut pipe = redis::pipe();
        config.save_pubkeys(&mut pipe).await?;
        pipe.set(&self.config.config, serde_json::to_string(config)?);
        pipe.query_async(&mut connection).await?;
        Ok(())
    }

    pub async fn send_message(&self, message: &ConfigMgmtMsg) -> AdminResult<usize> {
        let mut connection = self.connection.lock().await;
        Self::send_message2(&mut *connection, &self.config.channel, message).await
    }

    async fn send_message2(
        connection: &mut Connection,
        channel: &str,
        message: &ConfigMgmtMsg,
    ) -> AdminResult<usize> {
        connection
            .publish(channel, serde_json::to_string(message)?)
            .await
            .map_err(Into::into)
    }

    async fn heartbeat_loop(
        mut connection: Connection,
        mut shutdown: oneshot::Receiver<()>,
        channel: String,
        node: String,
    ) {
        let mut message = ConfigMgmtMsg::Request {
            node: Some(node),
            id: 0,
            action: ConfigMgmtMsgRequest::Heartbeat,
        };

        set_heath(HealthInfoType::RedisHeartbeat, Ok(()));
        loop {
            tokio::select! {
                _ = time::sleep(time::Duration::from_secs(10)) => {
                    if let Err(error) = Self::send_message2(&mut connection, &channel, &message).await {
                        error!("heartbeat error: {:?}", error);
                    }
                    if let ConfigMgmtMsg::Request{ id, ..} = &mut message {
                        *id = id.wrapping_add(1);
                    }
                },
                _ = &mut shutdown => {
                    break;
                }
            }
        }
        set_heath(HealthInfoType::RedisHeartbeat, Err(()));
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
    pub fn as_str(&self) -> &str {
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
    pub fn as_str(&self) -> &str {
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
