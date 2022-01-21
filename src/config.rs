use {
    super::sqs::SlotStatus,
    rusoto_core::Region,
    serde::{de, Deserialize, Deserializer},
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, Result as PluginResult,
    },
    solana_sdk::pubkey::Pubkey,
    std::{fs::read_to_string, path::Path},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    pub log: ConfigLog,
    pub sqs: ConfigAwsSqs,
    pub filters: Vec<ConfigAccountsFilter>,
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
    pub commitment_level: SlotStatus,
    pub url: String,
    #[serde(deserialize_with = "deserialize_region")]
    pub region: Region,
    pub auth: ConfigAwsAuth,
    #[serde(deserialize_with = "deserialize_max_requests")]
    pub max_requests: u64,
}

fn deserialize_region<'de, D>(deserializer: D) -> Result<Region, D::Error>
where
    D: Deserializer<'de>,
{
    let value: &str = Deserialize::deserialize(deserializer)?;
    value.parse().map_err(de::Error::custom)
}

fn deserialize_max_requests<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(match u64::deserialize(deserializer)? {
        0 => u64::MAX,
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

#[derive(Debug)]
pub struct ConfigAccountsFilter {
    pub owner: Option<Pubkey>,
    pub data_size: Option<usize>,
}

impl<'de> Deserialize<'de> for ConfigAccountsFilter {
    fn deserialize<D>(deserializer: D) -> Result<ConfigAccountsFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigAccountsFilterRaw {
            owner: Option<String>,
            data_size: Option<usize>,
        }

        let data = ConfigAccountsFilterRaw::deserialize(deserializer)?;
        if data.owner.is_none() && data.data_size.is_none() {
            Err(de::Error::custom(
                "At least one field in filter should be defined",
            ))
        } else {
            Ok(ConfigAccountsFilter {
                owner: match data.owner {
                    Some(value) => Some(value.parse().map_err(de::Error::custom)?),
                    None => None,
                },
                data_size: data.data_size,
            })
        }
    }
}
