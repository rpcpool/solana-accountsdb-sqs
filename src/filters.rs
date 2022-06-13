use {
    crate::{
        admin::{AdminError, ConfigMgmt, ConfigMgmtMsg, ConfigMgmtMsgTransactions},
        config::{
            ConfigAccountsFilter, ConfigFilters, ConfigTransactionsAccountsFilter,
            ConfigTransactionsFilter,
        },
        sqs::{ReplicaAccountInfo, ReplicaTransactionInfo},
    },
    futures::stream::{Stream, StreamExt},
    log::*,
    solana_sdk::{program_pack::Pack, pubkey::Pubkey},
    spl_token::state::Account as SplTokenAccount,
    std::{
        collections::{HashMap, HashSet},
        hash::Hash,
        sync::Arc,
    },
    thiserror::Error,
    tokio::sync::{oneshot, Mutex},
};

#[derive(Debug, Error)]
pub enum FiltersError {
    #[error("config management error: {0}")]
    Admin(#[from] AdminError),
}

pub type FiltersResult<T = ()> = Result<T, FiltersError>;

#[derive(Debug)]
pub struct Filters {
    config: ConfigFilters,
    accounts: AccountsFilter,
    transactions: Arc<Mutex<TransactionsFilter>>,
    shutdown: oneshot::Sender<()>,
}

impl Filters {
    pub async fn new(mut config: ConfigFilters, logs: bool) -> FiltersResult<Self> {
        let admin_with_pubsub = match &config.admin {
            Some(admin_config) => {
                let (admin, pubsub) = ConfigMgmt::new_with_pubsub(
                    admin_config.redis.clone(),
                    &admin_config.channel,
                    admin_config.lock_key.clone(),
                )
                .await?;
                if let Some(config_key) = &admin_config.config {
                    config = admin.get_global_config(config_key.clone()).await?;
                }
                Some((admin, pubsub))
            }
            None => None,
        };

        let accounts = AccountsFilter::new(&config.accounts);
        let transactions = Arc::new(Mutex::new(TransactionsFilter::new(
            config.transactions.clone(),
        )));

        if logs {
            info!("Init slots filter: {:?}", config.slots);
            info!("Init accounts filters: {:?}", accounts);
            info!("Init transactions filters: {:?}", transactions);
        }

        let (send, recv) = oneshot::channel();
        if let Some((admin, pubsub)) = admin_with_pubsub {
            tokio::spawn(Self::update_loop(
                admin,
                pubsub,
                logs,
                Arc::clone(&transactions),
                recv,
            ));
        }

        Ok(Self {
            config,
            accounts,
            transactions,
            shutdown: send,
        })
    }

    async fn update_loop(
        _admin: ConfigMgmt,
        pubsub: impl Stream<Item = ConfigMgmtMsg>,
        logs: bool,
        transactions: Arc<Mutex<TransactionsFilter>>,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        tokio::pin!(pubsub);
        loop {
            tokio::select! {
                msg = pubsub.next() => match msg {
                    Some(ConfigMgmtMsg::Transactions(msg)) => {
                        transactions.lock().await.handle_change(msg, logs);
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

    pub fn is_slot_messages_enabled(&self) -> bool {
        self.config.slots.enabled
    }

    pub fn contains_tokenkeg_owner(&self, owner: &Pubkey) -> bool {
        self.accounts.tokenkeg_owner.get(owner).is_some()
    }

    pub fn contains_tokenkeg_delegate(&self, owner: &Pubkey) -> bool {
        self.accounts.tokenkeg_delegate.get(owner).is_some()
    }

    pub fn create_accounts_match(&self) -> AccountsFilterMatch {
        AccountsFilterMatch {
            accounts_filter: &self.accounts,
            account: HashSet::new(),
            owner: HashSet::new(),
            data_size: HashSet::new(),
            tokenkeg_owner: HashSet::new(),
            tokenkeg_delegate: HashSet::new(),
        }
    }

    pub async fn get_transaction_filters(
        &self,
        transaction: &ReplicaTransactionInfo,
    ) -> Vec<String> {
        self.transactions.lock().await.get_filters(transaction)
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
    pub fn new(filters: &HashMap<String, ConfigAccountsFilter>) -> Self {
        let mut this = Self::default();
        for (name, filter) in filters.iter() {
            this.filters.push(name.clone());
            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                &filter.account,
            );
            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                &filter.owner,
            );
            Self::set(
                &mut this.data_size,
                &mut this.data_size_required,
                name,
                &filter.data_size,
            );
            Self::set(
                &mut this.tokenkeg_owner,
                &mut this.tokenkeg_owner_required,
                name,
                &filter.tokenkeg_owner,
            );
            Self::set(
                &mut this.tokenkeg_delegate,
                &mut this.tokenkeg_delegate_required,
                name,
                &filter.tokenkeg_delegate,
            );
        }
        this
    }

    fn set<Q: Hash + Eq + Clone>(
        map: &mut HashMap<Q, HashSet<String>>,
        set_required: &mut HashSet<String>,
        name: &str,
        set: &HashSet<Q>,
    ) {
        if !set.is_empty() {
            set_required.insert(name.to_string());
            for key in set.iter().cloned() {
                map.entry(key).or_default().insert(name.to_string());
            }
        }
    }
}

#[derive(Debug)]
pub struct AccountsFilterMatch<'a> {
    accounts_filter: &'a AccountsFilter,
    pub account: HashSet<&'a str>,
    pub owner: HashSet<&'a str>,
    pub data_size: HashSet<&'a str>,
    pub tokenkeg_owner: HashSet<&'a str>,
    pub tokenkeg_delegate: HashSet<&'a str>,
}

impl<'a> AccountsFilterMatch<'a> {
    fn extend<Q: Hash + Eq>(
        set: &mut HashSet<&'a str>,
        map: &'a HashMap<Q, HashSet<String>>,
        key: &Q,
    ) -> bool {
        if let Some(names) = map.get(key) {
            for name in names {
                set.insert(name);
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

// TODO: optimize filter (like accounts filter)
#[derive(Debug, Clone)]
struct TransactionsFilter {
    filters: HashMap<String, ConfigTransactionsFilter>,
}

impl TransactionsFilter {
    fn new(filters: HashMap<String, ConfigTransactionsFilter>) -> Self {
        Self { filters }
    }

    fn handle_change(&mut self, msg: ConfigMgmtMsgTransactions, logs: bool) {
        match msg {
            ConfigMgmtMsgTransactions::Add { name, config } => {
                if self.filters.contains_key(&name) {
                    error!("transactions filter already exists: {}", name);
                } else {
                    self.filters.insert(name.clone(), config);
                    if logs {
                        info!("transactions filter added: {}", name)
                    }
                }
            }
            ConfigMgmtMsgTransactions::Remove { name } => {
                if self.filters.remove(&name).is_some() && logs {
                    info!("transactions filter removed: {}", name);
                }
            }
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
        filter: &ConfigTransactionsFilter,
        transaction: &ReplicaTransactionInfo,
    ) -> bool {
        let ConfigTransactionsAccountsFilter { include, exclude } = &filter.accounts;
        let mut iter = transaction.transaction.message.account_keys.iter();

        if !include.is_empty() {
            return iter.any(|account_pubkey| include.contains(account_pubkey));
        }

        if !exclude.is_empty() {
            return iter.all(|account_pubkey| !exclude.contains(account_pubkey));
        }

        // No filters means that any transaction is ok
        true
    }
}
