use {
    crate::sqs::SlotStatus,
    flate2::{write::GzEncoder, Compression as GzCompression},
    redis::{aio::Connection as RedisConnection, AsyncCommands, Client as RedisClient, RedisError},
    rusoto_core::Region,
    serde::{de, ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        fs::read_to_string,
        hash::{Hash, Hasher},
        io::{Result as IoResult, Write},
        net::SocketAddr,
        path::Path,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum PubkeyWithSourceError {
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("expected loaded pubkeys for redis source")]
    ExpectedLoadedPubkeys,
}

pub type PubkeyWithSourceResult = Result<(), PubkeyWithSourceError>;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    #[serde(default)]
    pub log: ConfigLog,
    pub prometheus: Option<ConfigPrometheus>,
    pub sqs: ConfigAwsSqs,
    pub s3: ConfigAwsS3,
    pub messages: ConfigMessages,
    pub filters: ConfigFilters,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLog {
    /// Log level.
    #[serde(default = "ConfigLog::default_level")]
    pub level: String,
    /// Log filters on startup.
    pub filters: bool,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            filters: false,
        }
    }
}

impl ConfigLog {
    fn default_level() -> String {
        "info".to_owned()
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Address of Prometheus service.
    pub address: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigAwsSqs {
    pub auth: ConfigAwsAuth,
    #[serde(deserialize_with = "deserialize_region")]
    pub region: Region,
    pub url: String,
    #[serde(deserialize_with = "deserialize_max_requests")]
    pub max_requests: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, untagged)]
pub enum ConfigAwsAuth {
    Static {
        access_key_id: String,
        secret_access_key: String,
    },
    Chain {
        credentials_file: Option<String>,
        profile: Option<String>,
    },
}

fn deserialize_region<'de, D>(deserializer: D) -> Result<Region, D::Error>
where
    D: Deserializer<'de>,
{
    let value: &str = Deserialize::deserialize(deserializer)?;
    value.parse().map_err(de::Error::custom)
}

fn deserialize_max_requests<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match UsizeStr::deserialize(deserializer)?.value {
        0 => usize::MAX,
        value => value,
    })
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
struct UsizeStr {
    value: usize,
}

impl<'de> Deserialize<'de> for UsizeStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Integer(usize),
            String(String),
        }

        match Value::deserialize(deserializer)? {
            Value::Integer(value) => Ok(UsizeStr { value }),
            Value::String(value) => value
                .replace('_', "")
                .parse::<usize>()
                .map_err(de::Error::custom)
                .map(|value| UsizeStr { value }),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigAwsS3 {
    pub auth: ConfigAwsAuth,
    #[serde(deserialize_with = "deserialize_region")]
    pub region: Region,
    pub bucket: String,
    #[serde(deserialize_with = "deserialize_max_requests")]
    pub max_requests: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigMessages {
    #[serde(default, deserialize_with = "deserialize_commitment_level")]
    pub commitment_level: SlotStatus,
    #[serde(default)]
    pub accounts_data_compression: AccountsDataCompression,
}

fn deserialize_commitment_level<'de, D>(deserializer: D) -> Result<SlotStatus, D::Error>
where
    D: Deserializer<'de>,
{
    match Deserialize::deserialize(deserializer)? {
        SlotStatus::Processed => Err(de::Error::custom(
            "`commitment_level` as `processed` is not supported",
        )),
        value => Ok(value),
    }
}

#[derive(Debug, Clone, Copy, Deserialize, derivative::Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields, rename_all = "lowercase", tag = "algo")]
pub enum AccountsDataCompression {
    #[derivative(Default)]
    None,
    Zstd {
        #[serde(default)]
        level: i32,
    },
    Gzip {
        #[serde(default = "AccountsDataCompression::gzip_default_level")]
        level: u32,
    },
}

impl AccountsDataCompression {
    fn gzip_default_level() -> u32 {
        GzCompression::default().level()
    }

    #[allow(clippy::ptr_arg)]
    pub fn compress<'a>(&self, data: &'a Vec<u8>) -> IoResult<Cow<'a, Vec<u8>>> {
        Ok(match self {
            AccountsDataCompression::None => Cow::Borrowed(data),
            AccountsDataCompression::Zstd { level } => {
                Cow::Owned(zstd::stream::encode_all::<&[u8]>(data.as_ref(), *level)?)
            }
            AccountsDataCompression::Gzip { level } => {
                let mut encoder = GzEncoder::new(Vec::new(), GzCompression::new(*level));
                encoder.write_all(data)?;
                Cow::Owned(encoder.finish()?)
            }
        })
    }

    pub fn as_str(&self) -> &str {
        match *self {
            AccountsDataCompression::None => "none",
            AccountsDataCompression::Zstd { .. } => "zstd",
            AccountsDataCompression::Gzip { .. } => "gzip",
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigFilters {
    pub admin: Option<ConfigFiltersAdmin>,
    pub slots: ConfigSlotsFilter,
    pub accounts: HashMap<String, ConfigAccountsFilter>,
    pub transactions: HashMap<String, ConfigTransactionsFilter>,
}

impl ConfigFilters {
    pub async fn load_pubkeys(
        &mut self,
        connection: &mut RedisConnection,
    ) -> PubkeyWithSourceResult {
        for filter in self.accounts.values_mut() {
            filter.load_pubkeys(connection).await?;
        }
        Ok(())
    }

    pub async fn save_pubkeys(&self, connection: &mut RedisConnection) -> PubkeyWithSourceResult {
        for filter in self.accounts.values() {
            filter.save_pubkeys(connection).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFiltersAdmin {
    #[serde(deserialize_with = "deserialize_redis_client", skip_serializing)]
    pub redis: RedisClient,
    pub channel: String,
    pub config: String,
}

fn deserialize_redis_client<'de, D>(deserializer: D) -> Result<RedisClient, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)
        .and_then(|params| RedisClient::open(params).map_err(de::Error::custom))
}

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigSlotsFilter {
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, untagged)]
pub enum PubkeyWithSource {
    #[serde(
        deserialize_with = "PubkeyWithSource::deserialize_pubkey",
        serialize_with = "PubkeyWithSource::serialize_pubkey"
    )]
    Pubkey(Pubkey),
    Redis {
        set: String,
        keys: Option<HashSet<Pubkey>>,
    },
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for PubkeyWithSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Pubkey(pubkey) => pubkey.hash(state),
            Self::Redis { set, .. } => set.hash(state),
        }
    }
}

impl IntoIterator for PubkeyWithSource {
    type Item = Pubkey;
    type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Pubkey(pubkey) => Box::new(Some(pubkey).into_iter()),
            Self::Redis { keys, .. } => match keys {
                Some(keys) => Box::new(keys.into_iter()),
                None => Box::new(None.into_iter()),
            },
        }
    }
}

impl PubkeyWithSource {
    async fn load(&mut self, connection: &mut RedisConnection) -> PubkeyWithSourceResult {
        if let Self::Redis { set, keys } = self {
            let smembers: Vec<String> = connection.smembers(&*set).await?;
            *keys = Some(
                smembers
                    .iter()
                    .map(|member| member.parse().map_err(de::Error::custom))
                    .collect::<Result<_, serde_json::Error>>()?,
            );
        }
        Ok(())
    }

    async fn save(&self, connection: &mut RedisConnection) -> PubkeyWithSourceResult {
        if let Self::Redis { set, keys } = self {
            match keys {
                Some(keys) => {
                    let keys = keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
                    connection.del(&set).await?;
                    connection.sadd(&set, keys).await?;
                }
                None => return Err(PubkeyWithSourceError::ExpectedLoadedPubkeys),
            }
        }
        Ok(())
    }

    fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)
            .and_then(|pubkey| pubkey.parse().map_err(de::Error::custom))
    }

    fn serialize_pubkey<S>(pubkey: &Pubkey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(pubkey.to_string().as_str())
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAccountsFilter {
    pub account: HashSet<PubkeyWithSource>,
    pub owner: HashSet<PubkeyWithSource>,
    #[serde(deserialize_with = "deserialize_data_size")]
    pub data_size: HashSet<usize>,
    pub tokenkeg_owner: HashSet<PubkeyWithSource>,
    pub tokenkeg_delegate: HashSet<PubkeyWithSource>,
}

impl ConfigAccountsFilter {
    pub async fn load_pubkeys(
        &mut self,
        connection: &mut RedisConnection,
    ) -> PubkeyWithSourceResult {
        Self::load_pubkeys2(&mut self.account, connection).await
    }

    async fn load_pubkeys2(
        set: &mut HashSet<PubkeyWithSource>,
        connection: &mut RedisConnection,
    ) -> PubkeyWithSourceResult {
        let mut result = Ok(());
        for mut value in set.drain().collect::<Vec<_>>().into_iter() {
            if result.is_ok() {
                if let Err(error) = value.load(connection).await {
                    result = Err(error);
                }
            }
            set.insert(value);
        }
        result
    }

    pub async fn save_pubkeys(&self, connection: &mut RedisConnection) -> PubkeyWithSourceResult {
        for value in self.account.iter() {
            value.save(connection).await?;
        }
        Ok(())
    }
}

impl Serialize for ConfigAccountsFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("ConfigAccountsFilter", 5)?;
        s.serialize_field("account", &self.account)?;
        s.serialize_field("owner", &self.owner)?;
        s.serialize_field("data_size", &self.data_size)?;
        s.serialize_field("tokenkeg_owner", &self.tokenkeg_owner)?;
        s.serialize_field("tokenkeg_delegate", &self.tokenkeg_delegate)?;
        s.end()
    }
}

fn deserialize_data_size<'de, D>(deserializer: D) -> Result<HashSet<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    HashSet::<UsizeStr>::deserialize(deserializer)
        .map(|set| set.into_iter().map(|v| v.value).collect())
}

fn deserialize_set_pubkeys<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    HashSet::<&str>::deserialize(deserializer).and_then(|set| {
        set.into_iter()
            .map(|pubkey| pubkey.parse().map_err(de::Error::custom))
            .collect()
    })
}

fn serialize_set_pubkeys(set: &HashSet<Pubkey>) -> HashSet<String> {
    set.iter().map(|pubkey| pubkey.to_string()).collect()
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigTransactionsFilter {
    pub vote: bool,
    pub failed: bool,
    pub accounts: ConfigTransactionsAccountsFilter,
}

#[derive(Debug, Clone, Default)]
pub struct ConfigTransactionsAccountsFilter {
    pub include: HashSet<Pubkey>,
    pub exclude: HashSet<Pubkey>,
}

impl<'de> Deserialize<'de> for ConfigTransactionsAccountsFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Default, PartialEq, Eq, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct ConfigTransactionsAccountsFilterRaw {
            #[serde(deserialize_with = "deserialize_set_pubkeys")]
            include: HashSet<Pubkey>,
            #[serde(deserialize_with = "deserialize_set_pubkeys")]
            exclude: HashSet<Pubkey>,
        }

        let raw: ConfigTransactionsAccountsFilterRaw = Deserialize::deserialize(deserializer)?;
        if !raw.include.is_empty() && !raw.exclude.is_empty() {
            return Err(de::Error::custom(
                "`include` and `exlude` can not be used both at same moment",
            ));
        }

        Ok(ConfigTransactionsAccountsFilter {
            include: raw.include,
            exclude: raw.exclude,
        })
    }
}

impl Serialize for ConfigTransactionsAccountsFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("ConfigTransactionsAccountsFilter", 2)?;
        s.serialize_field("include", &serialize_set_pubkeys(&self.include))?;
        s.serialize_field("exclude", &serialize_set_pubkeys(&self.exclude))?;
        s.end()
    }
}
