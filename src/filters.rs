use {
    crate::{
        admin::{AdminError, ConfigMgmt, ConfigMgmtMsg},
        config::{
            ConfigAccountsFilter, ConfigFilters, ConfigSlotsFilter, ConfigTransactionsFilter,
        },
        sqs::{ReplicaAccountInfo, ReplicaTransactionInfo},
    },
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
    tokio::sync::{oneshot, MappedMutexGuard, Mutex, MutexGuard},
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
    pub async fn new(mut config: ConfigFilters, logs: bool) -> FiltersResult<Self> {
        let admin = match config.admin {
            Some(admin_config) => {
                let admin = ConfigMgmt::new(admin_config).await?;
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
            tokio::spawn(Self::update_loop(admin, recv, logs, Arc::clone(&inner)));
        }

        Ok(Self {
            inner,
            shutdown: send,
        })
    }

    async fn update_loop(
        mut admin: ConfigMgmt,
        mut shutdown: oneshot::Receiver<()>,
        logs: bool,
        inner: Arc<Mutex<FiltersInner>>,
    ) {
        loop {
            tokio::select! {
                msg = admin.pubsub.next() => match msg {
                    Some(ConfigMgmtMsg::Global) => {
                        let config = match admin.get_global_config().await {
                            Ok(config) => config,
                            Err(error) => {
                                error!("failed to read config on update: {:?}", error);
                                continue
                            },
                        };

                        let new_inner = FiltersInner::new(config);

                        let mut locked = inner.lock().await;
                        *locked = new_inner;
                        if logs {
                            info!("Update filters: {:?}", locked);
                        }
                    }
                    None => {
                        error!("filters changes subscription failed");
                        break;
                    }
                },
                _ = &mut shutdown => {
                    break;
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
struct AccountsFilter {
    filters: Vec<String>,
    account: HashMap<Pubkey, HashSet<String>>,
    account_required: HashSet<String>,
    owner: HashMap<Pubkey, HashSet<String>>,
    owner_required: HashSet<String>,
    data_size: HashMap<usize, HashSet<String>>,
    data_size_required: HashSet<String>,
    tokenkeg_owner: HashMap<Pubkey, HashSet<String>>,
    tokenkeg_owner_required: HashSet<String>,
    tokenkeg_delegate: HashMap<Pubkey, HashSet<String>>,
    tokenkeg_delegate_required: HashSet<String>,
}

impl AccountsFilter {
    pub fn new(filters: HashMap<String, ConfigAccountsFilter>) -> Self {
        let mut this = Self::default();
        for (name, filter) in filters.into_iter() {
            let mut not_empty = false;
            not_empty |= Self::set(
                &mut this.account,
                &mut this.account_required,
                &name,
                filter
                    .account
                    .into_iter()
                    .flat_map(|value| value.into_iter()),
            );
            not_empty |= Self::set(
                &mut this.owner,
                &mut this.owner_required,
                &name,
                filter.owner.into_iter().flat_map(|value| value.into_iter()),
            );
            not_empty |= Self::set(
                &mut this.data_size,
                &mut this.data_size_required,
                &name,
                filter.data_size.into_iter(),
            );
            not_empty |= Self::set(
                &mut this.tokenkeg_owner,
                &mut this.tokenkeg_owner_required,
                &name,
                filter
                    .tokenkeg_owner
                    .into_iter()
                    .flat_map(|value| value.into_iter()),
            );
            not_empty |= Self::set(
                &mut this.tokenkeg_delegate,
                &mut this.tokenkeg_delegate_required,
                &name,
                filter
                    .tokenkeg_delegate
                    .into_iter()
                    .flat_map(|value| value.into_iter()),
            );
            if not_empty {
                this.filters.push(name);
            }
        }
        this
    }

    fn set<Q, I>(
        map: &mut HashMap<Q, HashSet<String>>,
        set_required: &mut HashSet<String>,
        name: &str,
        keys: I,
    ) -> bool
    where
        Q: Hash + Eq + Clone,
        I: Iterator<Item = Q>,
    {
        let mut empty = true;
        for key in keys {
            empty = false;
            map.entry(key).or_default().insert(name.to_string());
        }

        if empty {
            false
        } else {
            set_required.insert(name.to_string());
            true
        }
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
        }
    }

    pub fn reset(&mut self) {
        self.account = Default::default();
        self.owner = Default::default();
        self.data_size = Default::default();
        self.tokenkeg_owner = Default::default();
        self.tokenkeg_delegate = Default::default();
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

    pub fn get_filters(&self) -> Vec<String> {
        self.accounts_filter
            .filters
            .iter()
            .filter(|name| {
                let name = name.as_str();
                let af = &self.accounts_filter;

                // If filter name in required but not in matched => return `false`
                if af.account_required.contains(name) && !self.account.contains(name) {
                    return false;
                }
                if af.owner_required.contains(name) && !self.owner.contains(name) {
                    return false;
                }
                if af.data_size_required.contains(name) && !self.data_size.contains(name) {
                    return false;
                }
                if af.tokenkeg_owner_required.contains(name) && !self.tokenkeg_owner.contains(name)
                {
                    return false;
                }
                if af.tokenkeg_delegate_required.contains(name)
                    && !self.tokenkeg_delegate.contains(name)
                {
                    return false;
                }

                true
            })
            .cloned()
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
