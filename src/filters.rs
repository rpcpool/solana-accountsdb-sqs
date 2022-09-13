use {
    crate::{
        admin::{
            AdminError, ConfigMgmt, ConfigMgmtMsg, ConfigMgmtMsgAction, ConfigMgmtMsgFilter,
            ConfigMgmtMsgFilterAccounts, ConfigMgmtMsgFilterTransactions, ConfigMgmtMsgRequest,
        },
        config::{
            ConfigAccountsFilter, ConfigFilters, ConfigRedis, ConfigSlotsFilter,
            ConfigTransactionsFilter,
        },
        prom::health::{set_health, HealthInfoType},
        serum::{self, EventFlag},
        sqs::{ReplicaAccountInfo, ReplicaTransactionInfo},
        version::VERSION,
    },
    enumflags2::BitFlags,
    futures::stream::StreamExt,
    log::*,
    solana_sdk::{program_pack::Pack, pubkey::Pubkey},
    spl_token::state::Account as SplTokenAccount,
    std::{
        collections::{HashMap, HashSet},
        hash::Hash,
        sync::Arc,
    },
    thiserror::Error,
    tokio::{
        sync::{oneshot, MappedMutexGuard, Mutex, MutexGuard},
        time::{sleep, Duration},
    },
};

#[derive(Debug, Error)]
pub enum FiltersError {
    #[error("config management error: {0}")]
    Admin(#[from] AdminError),
}

pub type FiltersResult<T = ()> = Result<T, FiltersError>;

#[derive(Debug)]
pub struct FiltersInner {
    slots: ConfigSlotsFilter,
    accounts: AccountsFilter,
    transactions: TransactionsFilter,
}

impl FiltersInner {
    fn new(config: ConfigFilters) -> Self {
        Self {
            slots: config.slots,
            accounts: AccountsFilter::new(config.accounts),
            transactions: TransactionsFilter::new(config.transactions),
        }
    }
}

#[derive(Debug)]
pub struct Filters {
    inner: Arc<Mutex<FiltersInner>>,
    shutdown: oneshot::Sender<()>,
}

impl Filters {
    pub async fn new(
        mut config: ConfigFilters,
        redis: Option<ConfigRedis>,
        node: String,
        logs: bool,
    ) -> FiltersResult<Self> {
        let admin = match redis {
            Some(admin_config) => {
                let admin = ConfigMgmt::new(admin_config, Some(node.clone())).await?;
                config = admin.get_global_config().await?;
                Some(admin)
            }
            None => None,
        };

        let inner = Arc::new(Mutex::new(FiltersInner::new(config)));
        if logs {
            info!("Filters: {:?}", inner);
        }

        let (send, recv) = oneshot::channel();
        if let Some(admin) = admin {
            tokio::spawn(Self::update_loop(
                admin,
                recv,
                node,
                logs,
                Arc::clone(&inner),
            ));
        }

        Ok(Self {
            inner,
            shutdown: send,
        })
    }

    async fn update_loop(
        mut admin: ConfigMgmt,
        mut shutdown: oneshot::Receiver<()>,
        this_node: String,
        logs: bool,
        inner: Arc<Mutex<FiltersInner>>,
    ) {
        async fn send_message(admin: &mut ConfigMgmt, message: &ConfigMgmtMsg) {
            let status = match admin.send_message(message).await {
                Ok(receivers) => {
                    debug!("message sent to {} receivers", receivers);
                    Ok(())
                }
                Err(error) => {
                    error!("failed to send admin message: {:?}", error);
                    Err(())
                }
            };
            set_health(HealthInfoType::RedisAdmin, status);
        }

        loop {
            // Create pubsub
            let mut pubsub = match admin.get_pubsub().await {
                Ok(pubsub) => {
                    set_health(HealthInfoType::RedisAdmin, Ok(()));
                    info!("admin pubsub created");
                    pubsub
                }
                Err(error) => {
                    set_health(HealthInfoType::RedisAdmin, Err(()));
                    error!("failed to subscribe on message updates: {:?}", error);
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };
            send_message(
                &mut admin,
                &ConfigMgmtMsg::Response {
                    node: this_node.clone(),
                    id: None,
                    result: Some("subscribed on updates".to_owned()),
                    error: None,
                },
            )
            .await;

            // Update global config from Redis
            let (result, error) = match admin.get_global_config().await {
                Ok(config) => {
                    let new_inner = FiltersInner::new(config);
                    let mut locked = inner.lock().await;
                    *locked = new_inner;
                    set_health(HealthInfoType::RedisAdmin, Ok(()));
                    info!("config loaded from redis and updated");
                    (Some("config updated".to_owned()), None)
                }
                Err(error) => {
                    set_health(HealthInfoType::RedisAdmin, Err(()));
                    error!("failed to load global config in pubsub: {:?}", error);
                    (
                        None,
                        Some(format!("failed to load global config: {:?}", error)),
                    )
                }
            };
            send_message(
                &mut admin,
                &ConfigMgmtMsg::Response {
                    node: this_node.clone(),
                    id: None,
                    result,
                    error: error.clone(),
                },
            )
            .await;
            if error.is_some() {
                set_health(HealthInfoType::RedisAdmin, Err(()));
                sleep(Duration::from_secs(10)).await;
                continue;
            }

            // Handle requests
            loop {
                tokio::select! {
                    msg = pubsub.next() => {
                        set_health(HealthInfoType::RedisAdmin, Ok(()));
                        match msg {
                            Some(ConfigMgmtMsg::Request { action: ConfigMgmtMsgRequest::Heartbeat, .. }) => {},
                            Some(ConfigMgmtMsg::Request { node, id, action: ConfigMgmtMsgRequest::Ping }) => if node.is_none() || node.as_deref() == Some(this_node.as_str()) {
                                send_message(&mut admin, &ConfigMgmtMsg::Response {
                                    node: this_node.clone(),
                                    id: Some(id),
                                    result: Some("pong".to_owned()),
                                    error: None
                                }).await;
                            }
                            Some(ConfigMgmtMsg::Request { node, id, action: ConfigMgmtMsgRequest::Version }) => if node.is_none() || node.as_deref() == Some(this_node.as_str()) {
                                send_message(&mut admin, &ConfigMgmtMsg::Response {
                                    node: this_node.clone(),
                                    id: Some(id),
                                    result: Some(serde_json::to_string(&VERSION).unwrap()),
                                    error: None
                                }).await;
                            }
                            Some(ConfigMgmtMsg::Request { node, id, action: ConfigMgmtMsgRequest::Global }) => if node.is_none() || node.as_deref() == Some(this_node.as_str()) {
                                let updated = match admin.get_global_config().await {
                                    Ok(config) => {
                                        let new_inner = FiltersInner::new(config);

                                        let mut locked = inner.lock().await;
                                        *locked = new_inner;
                                        if logs {
                                            info!("Update filters: {:?}", locked);
                                        }

                                        Ok(())
                                    },
                                    Err(error) => Err(format!("failed to read config on update: {:?}", error))
                                };

                                let (result, error) = match updated {
                                    Ok(()) => (Some("ok".to_owned()), None),
                                    Err(error) => {
                                        if logs {
                                            error!("{}", error);
                                        }
                                        (None, Some(error))
                                    }
                                };

                                send_message(&mut admin, &ConfigMgmtMsg::Response {
                                    node: this_node.clone(),
                                    id: Some(id),
                                    result,
                                    error,
                                }).await;
                            }
                            Some(ConfigMgmtMsg::Request { node, id, action: ConfigMgmtMsgRequest::PubkeysSet { filter, action, pubkey } }) => if node.is_none() || node.as_deref() == Some(this_node.as_str()) {
                                let mut locked = inner.lock().await;
                                let updated = match filter {
                                    ConfigMgmtMsgFilter::Accounts { name, kind } => {
                                        locked.accounts.change_pubkeys(name, kind, action, pubkey, logs)
                                    },
                                    ConfigMgmtMsgFilter::Transactions {name, kind } => {
                                        locked.transactions.change_pubkeys(name, kind, action, pubkey, logs)
                                    },
                                };
                                drop(locked);

                                let (result, error) = match updated {
                                    Ok(()) => (Some("ok".to_owned()), None),
                                    Err(error) => {
                                        if logs {
                                            error!("{}", error);
                                        }
                                        (None, Some(error))
                                    }
                                };

                                send_message(&mut admin, &ConfigMgmtMsg::Response {
                                    node: this_node.clone(),
                                    id: Some(id),
                                    result,
                                    error,
                                }).await;
                            }
                            Some(ConfigMgmtMsg::Response { .. }) => {},
                            None => {
                                set_health(HealthInfoType::RedisAdmin, Err(()));
                                error!("admin subscription finished");
                                break;
                            }
                        }
                    },
                    // Heartbeat each 10s, so 12s should be good
                    _ = sleep(Duration::from_secs(12)) => {
                        set_health(HealthInfoType::RedisAdmin, Err(()));
                        error!("admin subscription timeout");
                        break;
                    }
                    _ = &mut shutdown => {
                        set_health(HealthInfoType::RedisAdmin, Err(()));
                        admin.shutdown();
                        return;
                    }
                }
            }
        }
    }

    pub fn shutdown(self) {
        let _ = self.shutdown.send(());
    }

    pub async fn is_slot_messages_enabled(&self) -> bool {
        self.inner.lock().await.slots.enabled
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn create_accounts_match<'a>(&'a self) -> AccountsFilterMatch<'a> {
        let inner = self.inner.lock().await;
        AccountsFilterMatch::new(MutexGuard::map(inner, |inner| &mut inner.accounts))
    }

    pub async fn get_transaction_filters(
        &self,
        transaction: &ReplicaTransactionInfo,
    ) -> Vec<String> {
        let inner = self.inner.lock().await;
        inner.transactions.get_filters(transaction)
    }
}

#[derive(Debug, Default, Clone)]
struct AccountsFilterExistence {
    account: bool,
    owner: bool,
    data_size: bool,
    tokenkeg_owner: bool,
    tokenkeg_delegate: bool,
    serum_event_queue: bool,
}

impl AccountsFilterExistence {
    fn is_empty(&self) -> bool {
        !(self.account
            || self.owner
            || self.data_size
            || self.tokenkeg_owner
            || self.tokenkeg_delegate
            || self.serum_event_queue)
    }
}

#[derive(Debug, Default, Clone)]
struct AccountsFilter {
    filters: HashMap<String, AccountsFilterExistence>,
    account: HashMap<Pubkey, HashSet<String>>,
    account_required: HashMap<String, usize>,
    owner: HashMap<Pubkey, HashSet<String>>,
    owner_required: HashMap<String, usize>,
    data_size: HashMap<usize, HashSet<String>>,
    data_size_required: HashMap<String, usize>,
    tokenkeg_owner: HashMap<Pubkey, HashSet<String>>,
    tokenkeg_owner_required: HashMap<String, usize>,
    tokenkeg_delegate: HashMap<Pubkey, HashSet<String>>,
    tokenkeg_delegate_required: HashMap<String, usize>,
    serum_event_queue: HashMap<(Pubkey, Vec<BitFlags<EventFlag>>), HashSet<String>>,
    serum_event_queue_required: HashMap<String, usize>,
}

impl AccountsFilter {
    pub fn new(filters: HashMap<String, ConfigAccountsFilter>) -> Self {
        let mut this = Self::default();
        for (name, filter) in filters.into_iter() {
            let serum_event_queue_events = filter
                .serum_event_queue
                .events
                .iter()
                .cloned()
                .collect::<Vec<_>>();

            let existence = AccountsFilterExistence {
                account: Self::set(
                    &mut this.account,
                    &mut this.account_required,
                    &name,
                    filter
                        .account
                        .into_iter()
                        .flat_map(|value| value.into_iter()),
                ),
                owner: Self::set(
                    &mut this.owner,
                    &mut this.owner_required,
                    &name,
                    filter.owner.into_iter().flat_map(|value| value.into_iter()),
                ),
                data_size: Self::set(
                    &mut this.data_size,
                    &mut this.data_size_required,
                    &name,
                    filter.data_size.into_iter(),
                ),
                tokenkeg_owner: Self::set(
                    &mut this.tokenkeg_owner,
                    &mut this.tokenkeg_owner_required,
                    &name,
                    filter
                        .tokenkeg_owner
                        .into_iter()
                        .flat_map(|value| value.into_iter()),
                ),
                tokenkeg_delegate: Self::set(
                    &mut this.tokenkeg_delegate,
                    &mut this.tokenkeg_delegate_required,
                    &name,
                    filter
                        .tokenkeg_delegate
                        .into_iter()
                        .flat_map(|value| value.into_iter()),
                ),
                serum_event_queue: Self::set(
                    &mut this.serum_event_queue,
                    &mut this.serum_event_queue_required,
                    &name,
                    filter
                        .serum_event_queue
                        .accounts
                        .into_iter()
                        .flat_map(|value| value.into_iter())
                        .map(|pubkey| (pubkey, serum_event_queue_events.clone())),
                ),
            };
            this.filters.insert(name, existence);
        }
        this
    }

    fn set<Q, I>(
        map: &mut HashMap<Q, HashSet<String>>,
        map_required: &mut HashMap<String, usize>,
        name: &str,
        keys: I,
    ) -> bool
    where
        Q: Hash + Eq + Clone,
        I: Iterator<Item = Q>,
    {
        let mut count = 0;
        for key in keys {
            if map.entry(key).or_default().insert(name.to_string()) {
                count += 1;
            }
        }

        if count > 0 {
            map_required.insert(name.to_string(), count);
            true
        } else {
            false
        }
    }

    pub fn change_pubkeys(
        &mut self,
        name: String,
        target: ConfigMgmtMsgFilterAccounts,
        action: ConfigMgmtMsgAction,
        pubkey: Pubkey,
        logs: bool,
    ) -> Result<(), String> {
        let existence = match self.filters.get_mut(&name) {
            Some(value) => value,
            None => return Err(format!("filter {} not found", name)),
        };

        let (map, map_required, existence_field) = match target {
            ConfigMgmtMsgFilterAccounts::Account => (
                &mut self.account,
                &mut self.account_required,
                &mut existence.account,
            ),
            ConfigMgmtMsgFilterAccounts::Owner => (
                &mut self.owner,
                &mut self.owner_required,
                &mut existence.owner,
            ),
            ConfigMgmtMsgFilterAccounts::TokenkegOwner => (
                &mut self.tokenkeg_owner,
                &mut self.tokenkeg_owner_required,
                &mut existence.tokenkeg_owner,
            ),
            ConfigMgmtMsgFilterAccounts::TokenkegDelegate => (
                &mut self.tokenkeg_delegate,
                &mut self.tokenkeg_delegate_required,
                &mut existence.tokenkeg_delegate,
            ),
        };

        if let Some(action) = match action {
            ConfigMgmtMsgAction::Add => {
                let set = map.entry(pubkey).or_default();
                if !set.insert(name.clone()) {
                    return Err(format!(
                        "pubkey {} in filter {}.{} already exists",
                        pubkey,
                        name,
                        target.as_str()
                    ));
                }

                let value = map_required.entry(name.clone()).or_default();
                *value += 1;
                *existence_field = true;
                logs.then(|| "added to")
            }
            ConfigMgmtMsgAction::Remove => {
                let set = match map.get_mut(&pubkey) {
                    Some(set) if set.contains(&name) => set,
                    _ => {
                        return Err(format!(
                            "pubkey {} in filter {}.{} not exists",
                            pubkey,
                            name,
                            target.as_str()
                        ))
                    }
                };

                set.remove(&name);
                if set.is_empty() {
                    map.remove(&pubkey);
                }

                if let Some(value) = map_required.get_mut(&name) {
                    *value -= 1;
                    if *value == 0 {
                        *existence_field = false;
                    }
                }
                if !*existence_field {
                    map_required.remove(&name);
                }

                logs.then(|| "removed from")
            }
        } {
            info!(
                "{} {} the accounts filter {:?}.{:?}",
                pubkey,
                action,
                name,
                target.as_str()
            );
            warn!("config: {:?}", *self);
        }

        Ok(())
    }
}

// Is it possible todo with `&str` instead of `String`?
#[derive(Debug)]
pub struct AccountsFilterMatch<'a> {
    accounts_filter: MappedMutexGuard<'a, AccountsFilter>,
    account: HashSet<String>,
    owner: HashSet<String>,
    data_size: HashSet<String>,
    tokenkeg_owner: HashSet<String>,
    tokenkeg_delegate: HashSet<String>,
    serum_event_queue: HashSet<String>,
}

impl<'a> AccountsFilterMatch<'a> {
    fn new(accounts_filter: MappedMutexGuard<'a, AccountsFilter>) -> Self {
        Self {
            accounts_filter,
            account: Default::default(),
            owner: Default::default(),
            data_size: Default::default(),
            tokenkeg_owner: Default::default(),
            tokenkeg_delegate: Default::default(),
            serum_event_queue: Default::default(),
        }
    }

    pub fn reset(&mut self) {
        self.account = Default::default();
        self.owner = Default::default();
        self.data_size = Default::default();
        self.tokenkeg_owner = Default::default();
        self.tokenkeg_delegate = Default::default();
        self.serum_event_queue = Default::default();
    }

    pub fn contains_tokenkeg_owner(&self, owner: &Pubkey) -> bool {
        self.accounts_filter.tokenkeg_owner.get(owner).is_some()
    }

    pub fn contains_tokenkeg_delegate(&self, owner: &Pubkey) -> bool {
        self.accounts_filter.tokenkeg_delegate.get(owner).is_some()
    }

    fn extend<Q: Hash + Eq>(
        set: &mut HashSet<String>,
        map: &HashMap<Q, HashSet<String>>,
        key: &Q,
    ) -> bool {
        if let Some(names) = map.get(key) {
            for name in names {
                if !set.contains(name) {
                    set.insert(name.clone());
                }
            }
            true
        } else {
            false
        }
    }

    pub fn match_account(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(&mut self.account, &self.accounts_filter.account, pubkey)
    }

    pub fn match_owner(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(&mut self.owner, &self.accounts_filter.owner, pubkey)
    }

    pub fn match_data_size(&mut self, data_size: usize) -> bool {
        Self::extend(
            &mut self.data_size,
            &self.accounts_filter.data_size,
            &data_size,
        )
    }

    // Any Tokenkeg Account
    pub fn match_tokenkeg(&self, account: &ReplicaAccountInfo) -> bool {
        account.owner == spl_token::ID
            && account.data.len() == SplTokenAccount::LEN
            && (!self.accounts_filter.tokenkeg_owner.is_empty()
                || !self.accounts_filter.tokenkeg_delegate.is_empty())
    }

    pub fn match_tokenkeg_owner(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(
            &mut self.tokenkeg_owner,
            &self.accounts_filter.tokenkeg_owner,
            pubkey,
        )
    }

    pub fn match_tokenkeg_delegate(&mut self, pubkey: &Pubkey) -> bool {
        Self::extend(
            &mut self.tokenkeg_delegate,
            &self.accounts_filter.tokenkeg_delegate,
            pubkey,
        )
    }

    pub fn match_serum_event_queue(&mut self, pubkey: &Pubkey, data: &[u8]) -> bool {
        let mut vec = vec![];
        for ((key, events), name) in self.accounts_filter.serum_event_queue.iter() {
            if key == pubkey {
                vec.push((events, name));
            }
        }

        let matched = serum::match_events(data, &vec);
        let found = !matched.is_empty();
        for name in matched {
            self.serum_event_queue.insert(name);
        }
        found
    }

    pub fn get_filters(&self) -> Vec<String> {
        self.accounts_filter
            .filters
            .iter()
            .filter_map(|(name, existence)| {
                if existence.is_empty() {
                    return None;
                }

                let name = name.as_str();
                let af = &self.accounts_filter;

                // If filter name in required but not in matched => return `false`
                if af.account_required.contains_key(name) && !self.account.contains(name) {
                    return None;
                }
                if af.owner_required.contains_key(name) && !self.owner.contains(name) {
                    return None;
                }
                if af.data_size_required.contains_key(name) && !self.data_size.contains(name) {
                    return None;
                }
                if af.tokenkeg_owner_required.contains_key(name)
                    && !self.tokenkeg_owner.contains(name)
                {
                    return None;
                }
                if af.tokenkeg_delegate_required.contains_key(name)
                    && !self.tokenkeg_delegate.contains(name)
                {
                    return None;
                }
                if af.serum_event_queue_required.contains_key(name)
                    && !self.serum_event_queue.contains(name)
                {
                    return None;
                }

                Some(name.to_string())
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct TransactionsFilterInner {
    vote: bool,
    failed: bool,
    accounts_include: HashSet<Pubkey>,
    accounts_exclude: HashSet<Pubkey>,
}

// TODO: optimize filter (like accounts filter)
#[derive(Debug, Clone)]
struct TransactionsFilter {
    filters: HashMap<String, TransactionsFilterInner>,
}

impl TransactionsFilter {
    fn new(filters: HashMap<String, ConfigTransactionsFilter>) -> Self {
        Self {
            filters: filters
                .into_iter()
                .map(|(name, filter)| {
                    (
                        name,
                        TransactionsFilterInner {
                            vote: filter.vote,
                            failed: filter.failed,
                            accounts_include: filter
                                .accounts
                                .include
                                .into_iter()
                                .flat_map(|value| value.into_iter())
                                .collect(),
                            accounts_exclude: filter
                                .accounts
                                .exclude
                                .into_iter()
                                .flat_map(|value| value.into_iter())
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn change_pubkeys(
        &mut self,
        name: String,
        target: ConfigMgmtMsgFilterTransactions,
        action: ConfigMgmtMsgAction,
        pubkey: Pubkey,
        logs: bool,
    ) -> Result<(), String> {
        let filter = match self.filters.get_mut(&name) {
            Some(value) => value,
            None => return Err(format!("filter {} not found", name)),
        };

        let set = match target {
            ConfigMgmtMsgFilterTransactions::AccountsInclude => &mut filter.accounts_include,
            ConfigMgmtMsgFilterTransactions::AccountsExclude => &mut filter.accounts_exclude,
        };
        if let Some(action) = match action {
            ConfigMgmtMsgAction::Add => {
                if !set.insert(pubkey) {
                    return Err(format!(
                        "pubkey {} in filter {}.{} already exists",
                        pubkey,
                        name,
                        target.as_str()
                    ));
                }
                logs.then(|| "added to")
            }
            ConfigMgmtMsgAction::Remove => {
                if !set.remove(&pubkey) {
                    return Err(format!(
                        "pubkey {} in filter {}.{} not exists",
                        pubkey,
                        name,
                        target.as_str()
                    ));
                }
                logs.then(|| "removed from")
            }
        } {
            info!(
                "{} {} the transcation filter {:?}.{:?}",
                pubkey,
                action,
                name,
                target.as_str()
            );
        }

        Ok(())
    }

    pub fn get_filters(&self, transaction: &ReplicaTransactionInfo) -> Vec<String> {
        self.filters
            .iter()
            .filter_map(|(name, filter)| {
                if transaction.is_vote && !filter.vote {
                    return None;
                }

                if transaction.meta.err.is_some() && !filter.failed {
                    return None;
                }

                if !Self::contains_program(filter, transaction) {
                    return None;
                }

                Some(name.clone())
            })
            .collect()
    }

    fn contains_program(
        filter: &TransactionsFilterInner,
        transaction: &ReplicaTransactionInfo,
    ) -> bool {
        let mut iter = transaction.transaction.message.account_keys.iter();

        if !filter.accounts_include.is_empty() {
            return iter.any(|account_pubkey| filter.accounts_include.contains(account_pubkey));
        }

        if !filter.accounts_exclude.is_empty() {
            return iter.all(|account_pubkey| !filter.accounts_exclude.contains(account_pubkey));
        }

        // No filters means that any transaction is ok
        true
    }
}
