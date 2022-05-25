use {
    super::sqs::SlotStatus,
    rusoto_core::Region,
    serde::{de, Deserialize, Deserializer},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        fs::read_to_string,
        path::Path,
    },
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    pub log: ConfigLog,
    pub sqs: ConfigAwsSqs,
    pub slots: ConfigSlots,
    #[serde(rename = "accounts")]
    pub accounts_filters: HashMap<String, ConfigAccountsFilter>,
    #[serde(rename = "transactions")]
    pub transactions_filter: ConfigTransactionsFilter,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLog {
    pub level: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigAwsSqs {
    #[serde(default, deserialize_with = "deserialize_commitment_level")]
    pub commitment_level: SlotStatus,
    pub url: String,
    #[serde(deserialize_with = "deserialize_region")]
    pub region: Region,
    pub auth: ConfigAwsAuth,
    #[serde(deserialize_with = "deserialize_max_requests")]
    pub max_requests: usize,
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
    Ok(match usize::deserialize(deserializer)? {
        0 => usize::MAX,
        value => value,
    })
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

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ConfigSlots {
    pub enabled: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ConfigAccountsFilter {
    pub owner: HashSet<Pubkey>,
    pub account: HashSet<Pubkey>,
    pub data_size: HashSet<usize>,
    pub tokenkeg_owner: HashSet<Pubkey>,
    pub tokenkeg_delegate: HashSet<Pubkey>,
}

impl<'de> Deserialize<'de> for ConfigAccountsFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Default, PartialEq, Eq, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct ConfigAccountsFilterRaw {
            owner: HashSet<String>,
            account: HashSet<String>,
            data_size: HashSet<usize>,
            tokenkeg_owner: HashSet<String>,
            tokenkeg_delegate: HashSet<String>,
        }

        let raw: ConfigAccountsFilterRaw = Deserialize::deserialize(deserializer)?;
        if raw == ConfigAccountsFilterRaw::default() {
            return Err(de::Error::custom(
                "At least one field in filter should be defined",
            ));
        }

        let mut filter = ConfigAccountsFilter {
            data_size: raw.data_size,
            ..Default::default()
        };

        for pubkey in raw.owner.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.owner.insert(pubkey);
        }

        for pubkey in raw.account.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.account.insert(pubkey);
        }

        for pubkey in raw.tokenkeg_owner.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.tokenkeg_owner.insert(pubkey);
        }

        for pubkey in raw.tokenkeg_delegate.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.tokenkeg_delegate.insert(pubkey);
        }

        Ok(filter)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigTransactionsFilter {
    pub enabled: bool,
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
            include: HashSet<String>,
            exclude: HashSet<String>,
        }

        let raw: ConfigTransactionsAccountsFilterRaw = Deserialize::deserialize(deserializer)?;
        if !raw.include.is_empty() && !raw.exclude.is_empty() {
            return Err(de::Error::custom(
                "`include` and `exlude` can not be used both at same moment",
            ));
        }

        let mut filter = ConfigTransactionsAccountsFilter::default();

        for pubkey in raw.include.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.include.insert(pubkey);
        }

        for pubkey in raw.exclude.into_iter() {
            let pubkey = pubkey.parse().map_err(de::Error::custom)?;
            filter.exclude.insert(pubkey);
        }

        Ok(filter)
    }
}
