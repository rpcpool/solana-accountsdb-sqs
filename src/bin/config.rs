use {
    anyhow::{anyhow, Result},
    clap::{Parser, Subcommand},
    futures::stream::StreamExt,
    redis::AsyncCommands,
    solana_geyser_sqs::{
        admin::{
            ConfigMgmt, ConfigMgmtMsg, ConfigMgmtMsgAction, ConfigMgmtMsgFilter,
            ConfigMgmtMsgFilterAccounts, ConfigMgmtMsgFilterTransactions, ConfigMgmtMsgRequest,
        },
        config::{Config, ConfigAccountsFilter, ConfigTransactionsFilter, PubkeyWithSource},
        version::VERSION,
    },
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashSet, hash::Hash},
    tokio::time::{sleep, Duration},
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Path to geyser plugin config
    #[clap(short, long)]
    config: String,

    /// Filter for change
    #[clap(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Subcommand)]
enum ArgsAction {
    /// Load config to Redis
    Init,
    /// Print config from Redis
    Show,
    /// Change accounts filters
    #[clap(subcommand)]
    Accounts(ArgsActionAccounts),
    /// Change transactions filters
    #[clap(subcommand)]
    Transactions(ArgsActionTransactions),
    /// Change Public Keys in Set
    #[clap(subcommand)]
    Set(ArgsActionSet),
    /// Send update signal
    SendSignal {
        /// Node which will handle signal, or `None` for all nodes
        #[clap(short, long)]
        node: Option<String>,
        /// Signal kind
        #[clap(subcommand)]
        signal: ArgsActionSendSignal,
    },
    /// Watch for commands in Redis
    Watch,
    /// Print version info
    Version,
}

#[derive(Debug, Subcommand)]
enum ArgsActionAccounts {
    Add {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Optional config, otherwise
        #[clap(short, long)]
        config: Option<String>,
    },
    Change {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Add `Pubkey` to `account` in the filter
        #[clap(long)]
        account_add: Option<String>,
        /// Remove `Pubkey` from `account` in the filter
        #[clap(long)]
        account_remove: Option<String>,
        /// Add `Pubkey` to `owner` in the filter
        #[clap(long)]
        owner_add: Option<String>,
        /// Remove `Pubkey` from `owner` in the filter
        #[clap(long)]
        owner_remove: Option<String>,
        /// Add `size` to `data_size` in the filter
        #[clap(long)]
        data_size_add: Option<usize>,
        /// Remove `size` from `data_size` in the filter
        #[clap(long)]
        data_size_remove: Option<usize>,
        /// Add `Pubkey` to `tokenkeg_owner` in the filter
        #[clap(long)]
        tokenkeg_owner_add: Option<String>,
        /// Remove `Pubkey` from `tokenkeg_owner` in the filter
        #[clap(long)]
        tokenkeg_owner_remove: Option<String>,
        /// Add `Pubkey` to `tokenkeg_delegate` in the filter
        #[clap(long)]
        tokenkeg_delegate_add: Option<String>,
        /// Remove `Pubkey` from `tokenkeg_delegate` in the filter
        #[clap(long)]
        tokenkeg_delegate_remove: Option<String>,
    },
    Remove {
        /// Filter name
        #[clap(short, long)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum ArgsActionTransactions {
    Add {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Optional config, otherwise default
        #[clap(short, long)]
        config: Option<String>, // `value_parser`?
    },
    Change {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Change `vote` in the filter
        #[clap(short, long)]
        vote: Option<bool>,
        /// Change `failed` in the filter
        #[clap(short, long)]
        failed: Option<bool>,
        /// Add `Pubkey` to `accounts.include` in the filter
        #[clap(long)]
        account_add_include: Option<String>,
        /// Remove `Pubkey` from `accounts.include` in the filter
        #[clap(long)]
        account_remove_include: Option<String>,
        /// Add `Pubkey` to `accounts.exclude` in the filter
        #[clap(long)]
        account_add_exclude: Option<String>,
        /// Remove `Pubkey` from `accounts.exclude` in the filter
        #[clap(long)]
        account_remove_exclude: Option<String>,
    },
    Remove {
        /// Filter name
        #[clap(short, long)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ArgsActionSet {
    Add {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Public Key
        #[clap(short, long)]
        pubkey: String,
    },
    Remove {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Public Key
        #[clap(short, long)]
        pubkey: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ArgsActionSendSignal {
    /// Send ping
    Ping {
        /// Ping interval in seconds
        #[clap(short, long, default_value_t = 10)]
        interval: u64,
    },
    /// Ask plugin version info
    Version,
    /// Reload whole config
    Global,
    /// Add or remove Public Key
    PubkeysSet {
        /// Filter type: `accounts`, `transactions`
        #[clap(short, long)]
        filter: String,
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Kind of filter: `account`, `owner`, `tokenkeg_owner`, `tokenkeg_delegate`, `accounts_include`, `accounts_exclude`
        #[clap(short, long)]
        kind: String,
        /// Applied action: `add`, `remove`
        #[clap(short, long)]
        action: String,
        /// Public Key
        #[clap(short, long)]
        pubkey: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = Config::load_from_file(&args.config)?;
    let config_admin = config.redis.clone().expect("defined redis config");
    let admin = ConfigMgmt::new(config_admin.clone(), None).await?;

    match args.action {
        ArgsAction::Init => {
            let mut config = config.filters.clone();
            let mut connection = config_admin.url.get_async_connection().await?;
            config.load_pubkeys(&mut connection).await?;
            admin.set_global_config(&config).await?;
            println!("Config uploaded");
        }
        ArgsAction::Show => {
            println!("{:#?}", admin.get_global_config().await?);
        }
        ArgsAction::Accounts(action) => match action {
            ArgsActionAccounts::Add { name, config } => {
                let new_filter = match config {
                    Some(config) => serde_json::from_str(&config)?,
                    None => ConfigAccountsFilter::default(),
                };
                let mut config = admin.get_global_config().await?;
                let prev = config.accounts.insert(name.clone(), new_filter);
                admin.set_global_config(&config).await?;
                let action = if prev.is_some() { "updated" } else { "added" };
                println!("Accounts filter {:?} {}", name, action);
            }
            ArgsActionAccounts::Change {
                name,
                account_add,
                account_remove,
                owner_add,
                owner_remove,
                data_size_add,
                data_size_remove,
                tokenkeg_owner_add,
                tokenkeg_owner_remove,
                tokenkeg_delegate_add,
                tokenkeg_delegate_remove,
            } => {
                let mut config = admin.get_global_config().await?;
                if let Some(filter) = config.accounts.get_mut(&name) {
                    let mut changed = false;
                    changed |= set_add_pubkey(&mut filter.account, account_add)?;
                    changed |= set_remove_pubkey(&mut filter.account, account_remove)?;
                    changed |= set_add_pubkey(&mut filter.owner, owner_add)?;
                    changed |= set_remove_pubkey(&mut filter.owner, owner_remove)?;
                    changed |= set_add(&mut filter.data_size, data_size_add)?;
                    changed |= set_remove(&mut filter.data_size, data_size_remove)?;
                    changed |= set_add_pubkey(&mut filter.tokenkeg_owner, tokenkeg_owner_add)?;
                    changed |=
                        set_remove_pubkey(&mut filter.tokenkeg_owner, tokenkeg_owner_remove)?;
                    changed |=
                        set_add_pubkey(&mut filter.tokenkeg_delegate, tokenkeg_delegate_add)?;
                    changed |=
                        set_remove_pubkey(&mut filter.tokenkeg_delegate, tokenkeg_delegate_remove)?;
                    if changed {
                        admin.set_global_config(&config).await?;
                        println!("Accounts filter {:?} changed", name);
                    } else {
                        println!("Accounts filter {:?} nothing to update", name);
                    }
                } else {
                    println!("Accounts filter {:?} not found", name);
                }
            }
            ArgsActionAccounts::Remove { name } => {
                let mut config = admin.get_global_config().await?;
                if config.accounts.remove(&name).is_some() {
                    admin.set_global_config(&config).await?;
                    println!("Accounts filter {:?} removed", name);
                } else {
                    println!("Accounts filter {:?} not found in config", name);
                }
            }
        },
        ArgsAction::Transactions(action) => match action {
            ArgsActionTransactions::Add { name, config } => {
                let new_filter = match config {
                    Some(config) => serde_json::from_str(&config)?,
                    None => ConfigTransactionsFilter::default(),
                };
                let mut config = admin.get_global_config().await?;
                let prev = config.transactions.insert(name.clone(), new_filter);
                admin.set_global_config(&config).await?;
                let action = if prev.is_some() { "updated" } else { "added" };
                println!("Transaction filter {:?} {}", name, action);
            }
            ArgsActionTransactions::Change {
                name,
                vote,
                failed,
                account_add_include,
                account_remove_include,
                account_add_exclude,
                account_remove_exclude,
            } => {
                let mut config = admin.get_global_config().await?;
                if let Some(mut filter) = config.transactions.get_mut(&name) {
                    let mut changed = false;
                    match vote {
                        Some(vote) if filter.vote != vote => {
                            changed = true;
                            filter.vote = vote;
                        }
                        _ => {}
                    }
                    match failed {
                        Some(failed) if filter.failed != failed => {
                            changed = true;
                            filter.failed = failed;
                        }
                        _ => {}
                    }
                    changed |= set_add_pubkey(&mut filter.accounts.include, account_add_include)?;
                    changed |=
                        set_remove_pubkey(&mut filter.accounts.include, account_remove_include)?;
                    changed |= set_add_pubkey(&mut filter.accounts.exclude, account_add_exclude)?;
                    changed |=
                        set_remove_pubkey(&mut filter.accounts.exclude, account_remove_exclude)?;
                    if changed {
                        admin.set_global_config(&config).await?;
                        println!("Transaction filter {:?} changed", name);
                    } else {
                        println!("Transaction filter {:?} nothing to update", name);
                    }
                } else {
                    println!("Transaction filter {:?} not found", name);
                }
            }
            ArgsActionTransactions::Remove { name } => {
                let mut config = admin.get_global_config().await?;
                if config.transactions.remove(&name).is_some() {
                    admin.set_global_config(&config).await?;
                    println!("Transaction filter {:?} removed", name);
                } else {
                    println!("Transaction filter {:?} not found in config", name);
                }
            }
        },
        ArgsAction::Set(action) => match action {
            ArgsActionSet::Add { name, pubkey } => {
                pubkey.parse::<Pubkey>()?;
                let mut connection = admin.config.url.get_async_connection().await?;
                connection.sadd(&name, pubkey).await?;
            }
            ArgsActionSet::Remove { name, pubkey } => {
                pubkey.parse::<Pubkey>()?;
                let mut connection = admin.config.url.get_async_connection().await?;
                connection.srem(&name, pubkey).await?;
            }
        },
        ArgsAction::SendSignal { node, signal } => {
            let action = match signal {
                ArgsActionSendSignal::Ping { interval } => loop {
                    let id = rand::random::<u16>() as u64;
                    let msg = ConfigMgmtMsg::Request {
                        node: node.clone(),
                        id,
                        action: ConfigMgmtMsgRequest::Ping,
                    };
                    println!("Send message: {}", serde_json::to_string(&msg)?);
                    let receivers = admin.send_message(&msg).await?;
                    println!(
                        "{} subscribers received the message (1 of it, this tool itself)",
                        receivers
                    );

                    let mut pubsub = admin.get_pubsub().await?;
                    let sleep = sleep(Duration::from_secs(interval));
                    tokio::pin!(sleep);
                    loop {
                        tokio::select! {
                            msg = pubsub.next() => match msg {
                                Some(ConfigMgmtMsg::Response { node, id: rid, result, error }) if rid == Some(id) => {
                                    let msg = ConfigMgmtMsg::Response{ node: node.clone(), id: rid, result, error };
                                    println!("Received msg from node {:?}: {}", node, serde_json::to_string(&msg)?);
                                }
                                _ => {}
                            },
                            _ = &mut sleep => {
                                break
                            }
                        }
                    }
                },
                ArgsActionSendSignal::Version => ConfigMgmtMsgRequest::Version,
                ArgsActionSendSignal::Global => ConfigMgmtMsgRequest::Global,
                ArgsActionSendSignal::PubkeysSet {
                    filter,
                    name,
                    kind,
                    action,
                    pubkey,
                } => ConfigMgmtMsgRequest::PubkeysSet {
                    filter: match filter.as_str() {
                        "accounts" => ConfigMgmtMsgFilter::Accounts {
                            name,
                            kind: serde_json::from_value::<ConfigMgmtMsgFilterAccounts>(
                                serde_json::Value::String(kind),
                            )?,
                        },
                        "transactions" => ConfigMgmtMsgFilter::Transactions {
                            name,
                            kind: serde_json::from_value::<ConfigMgmtMsgFilterTransactions>(
                                serde_json::Value::String(kind),
                            )?,
                        },
                        filter => {
                            return Err(anyhow!(
                                "unknown variant `{}`, expected {:?}",
                                filter,
                                &["accounts", "transactions"]
                            ))
                        }
                    },
                    action: serde_json::from_value::<ConfigMgmtMsgAction>(
                        serde_json::Value::String(action),
                    )?,
                    pubkey: pubkey.parse::<Pubkey>()?,
                },
            };

            let id = rand::random::<u16>() as u64;
            let msg = ConfigMgmtMsg::Request { node, id, action };
            println!("Send message: {}", serde_json::to_string(&msg)?);
            let receivers = admin.send_message(&msg).await?;
            println!(
                "{} subscribers received the message (1 of it, this tool itself)",
                receivers
            );

            let mut received = 0;
            let mut pubsub = admin.get_pubsub().await?;
            let sleep = sleep(Duration::from_secs(30));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    msg = pubsub.next() => match msg {
                        Some(ConfigMgmtMsg::Response { node, id: rid, result, error }) if rid == Some(id) => {
                            let msg = ConfigMgmtMsg::Response{ node: node.clone(), id: rid, result, error };
                            println!("Received msg from node {:?}: {}", node, serde_json::to_string(&msg)?);
                            received += 1;
                        }
                        _ => {}
                    },
                    _ = &mut sleep => {
                        if received > 0 {
                            println!("Total received: {}, exit.", received);
                        } else {
                            eprintln!("failed to get response");
                        }
                        break
                    }
                }
            }
        }
        ArgsAction::Watch => {
            let mut pubsub = admin.get_pubsub().await?;
            while let Some(msg) = pubsub.next().await {
                println!("Received msg: {}", serde_json::to_string(&msg).unwrap());
            }
            println!("stream is finished");
        }
        ArgsAction::Version => {
            println!("{:#?}", VERSION);
        }
    }

    Ok(())
}

fn parse_pubkey_with_source(pubkey: String) -> PubkeyWithSource {
    match pubkey.parse() {
        Ok(pubkey) => PubkeyWithSource::Pubkey(pubkey),
        Err(_error) => PubkeyWithSource::Redis {
            set: pubkey,
            keys: Some(HashSet::new()),
        },
    }
}

fn set_add_pubkey(
    set: &mut HashSet<PubkeyWithSource>,
    pubkey_maybe: Option<String>,
) -> Result<bool> {
    Ok(if let Some(pubkey) = pubkey_maybe {
        set_add(set, Some(parse_pubkey_with_source(pubkey)))?
    } else {
        false
    })
}

fn set_add<T>(set: &mut HashSet<T>, value_maybe: Option<T>) -> Result<bool>
where
    T: Eq + Hash,
{
    Ok(if let Some(value) = value_maybe {
        let is_same = matches!(set.get(&value), Some(existed) if *existed == value);
        set.insert(value);
        !is_same
    } else {
        false
    })
}

fn set_remove_pubkey(
    set: &mut HashSet<PubkeyWithSource>,
    pubkey_maybe: Option<String>,
) -> Result<bool> {
    Ok(if let Some(pubkey) = pubkey_maybe {
        set_remove(set, Some(parse_pubkey_with_source(pubkey)))?
    } else {
        false
    })
}

fn set_remove<T>(set: &mut HashSet<T>, value_maybe: Option<T>) -> Result<bool>
where
    T: Eq + Hash,
{
    Ok(if let Some(value) = value_maybe {
        set.remove(&value)
    } else {
        false
    })
}
