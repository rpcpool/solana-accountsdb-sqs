use {
    crate::config::{ConfigFilters, ConfigFiltersAdmin, PubkeyWithSourceError},
    futures::stream::{Stream, StreamExt},
    log::*,
    redis::{AsyncCommands, RedisError},
    serde::{Deserialize, Serialize},
    std::pin::Pin,
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("expected admin value as null")]
    AdminNotNone,
    #[error("pubkeys failed with redis: {0}")]
    Pubkeys(#[from] PubkeyWithSourceError),
}

pub type AdminResult<T = ()> = Result<T, AdminError>;

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct ConfigMgmt {
    pub config: ConfigFiltersAdmin,
    #[derivative(Debug = "ignore")]
    pub pubsub: Pin<Box<dyn Stream<Item = ConfigMgmtMsg> + Send + Sync>>,
}

impl ConfigMgmt {
    pub async fn new(config: ConfigFiltersAdmin) -> AdminResult<Self> {
        let mut pubsub = config.redis.get_async_connection().await?.into_pubsub();
        pubsub.subscribe(&config.channel).await?;

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
        })
    }

    pub async fn get_global_config(&self) -> AdminResult<ConfigFilters> {
        let mut connection = self.config.redis.get_async_connection().await?;
        // TODO: use transaction?
        let data: String = connection.get(&self.config.config).await?;
        let mut config: ConfigFilters = serde_json::from_str(&data)?;
        if config.admin.is_none() {
            config.load_pubkeys(&mut connection).await?;
            Ok(config)
        } else {
            Err(AdminError::AdminNotNone)
        }
    }

    pub async fn set_global_config(&self, config: &ConfigFilters) -> AdminResult {
        if config.admin.is_none() {
            let mut connection = self.config.redis.get_async_connection().await?;
            let mut pipe = redis::pipe();
            config.save_pubkeys(&mut pipe).await?;
            pipe.set(&self.config.config, serde_json::to_string(config)?);
            pipe.query_async(&mut connection).await?;
            Ok(())
        } else {
            Err(AdminError::AdminNotNone)
        }
    }

    pub async fn send_message(&self, message: &ConfigMgmtMsg) -> AdminResult {
        let mut connection = self.config.redis.get_async_connection().await?;
        connection
            .publish(&self.config.channel, serde_json::to_string(message)?)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "target", rename_all = "snake_case")]
pub enum ConfigMgmtMsg {
    Global,
}
