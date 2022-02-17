use {
    super::sqs::SlotStatus,
    rusoto_core::Region,
    serde::{de, Deserialize, Deserializer},
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, Result as PluginResult,
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
    #[serde(rename = "filters")]
    pub filter: ConfigAccountsFilter,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| AccountsDbPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(AccountsDbPluginError::ConfigFileOpenError)?;
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
    File {
        credentials_file: String,
        profile: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ConfigSlots {
    pub messages: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ConfigAccountsFilterOwnerValue {
    pub without_size: bool,
    pub sizes: HashSet<usize>,
}

#[derive(Debug, Default, Clone)]
pub struct ConfigAccountsFilter {
    pub owner: HashMap<Pubkey, ConfigAccountsFilterOwnerValue>,
    pub data_size: HashSet<usize>,
    pub tokenkeg_owner: HashSet<Pubkey>,
    pub tokenkeg_delegate: HashSet<Pubkey>,
}

impl<'de> Deserialize<'de> for ConfigAccountsFilter {
    fn deserialize<D>(deserializer: D) -> Result<ConfigAccountsFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigAccountsFilterRaw<'a> {
            owner: Option<&'a str>,
            data_size: Option<usize>,
            tokenkeg_owner: Option<Vec<&'a str>>,
            tokenkeg_delegate: Option<Vec<&'a str>>,
        }

        let mut filter = ConfigAccountsFilter::default();
        let items: Vec<ConfigAccountsFilterRaw> = Deserialize::deserialize(deserializer)?;
        for item in items {
            if item.owner.is_none()
                && item.data_size.is_none()
                && item.tokenkeg_owner.is_none()
                && item.tokenkeg_delegate.is_none()
            {
                return Err(de::Error::custom(
                    "At least one field in filter should be defined",
                ));
            }
            if (item.owner.is_some() || item.data_size.is_some())
                && (item.tokenkeg_owner.is_some() || item.tokenkeg_delegate.is_some())
            {
                return Err(de::Error::custom(
                    "`tokenkeg_owner` or `tokenkeg_delegate` field is not compatiable with `owner` and `data_size`",
                ));
            }

            match (item.owner, item.data_size) {
                (None, None) => {}
                (None, Some(data_size)) => {
                    filter.data_size.insert(data_size);
                }
                (Some(owner), None) => {
                    let pubkey = owner.parse().map_err(de::Error::custom)?;
                    let entry = filter.owner.entry(pubkey).or_default();
                    entry.without_size = true;
                }
                (Some(owner), Some(data_size)) => {
                    let pubkey = owner.parse().map_err(de::Error::custom)?;
                    let entry = filter.owner.entry(pubkey).or_default();
                    entry.sizes.insert(data_size);
                }
            }

            if let Some(pubkeys) = item.tokenkeg_owner {
                for pubkey in pubkeys {
                    let pubkey = pubkey.parse().map_err(de::Error::custom)?;
                    filter.tokenkeg_owner.insert(pubkey);
                }
            }

            if let Some(pubkeys) = item.tokenkeg_delegate {
                for pubkey in pubkeys {
                    let pubkey = pubkey.parse().map_err(de::Error::custom)?;
                    filter.tokenkeg_delegate.insert(pubkey);
                }
            }
        }
        Ok(filter)
    }
}
