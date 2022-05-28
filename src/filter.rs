use {
    super::{
        config::{
            ConfigAccountsFilter, ConfigTransactionsAccountsFilter, ConfigTransactionsFilter,
        },
        sqs::{ReplicaAccountInfo, ReplicaTransactionInfo},
    },
    solana_sdk::{program_pack::Pack, pubkey::Pubkey},
    spl_token::state::Account as SplTokenAccount,
    std::{
        collections::{HashMap, HashSet},
        hash::Hash,
    },
};

#[derive(Debug, Default, Clone)]
pub struct AccountsFilter {
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
        for (name, filter) in filters {
            this.filters.push(name.clone());
            Self::set(
                &mut this.account,
                &mut this.account_required,
                &name,
                filter.account,
            );
            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                &name,
                filter.owner,
            );
            Self::set(
                &mut this.data_size,
                &mut this.data_size_required,
                &name,
                filter.data_size,
            );
            Self::set(
                &mut this.tokenkeg_owner,
                &mut this.tokenkeg_owner_required,
                &name,
                filter.tokenkeg_owner,
            );
            Self::set(
                &mut this.tokenkeg_delegate,
                &mut this.tokenkeg_delegate_required,
                &name,
                filter.tokenkeg_delegate,
            );
        }
        this
    }

    fn set<Q: Hash + Eq>(
        map: &mut HashMap<Q, HashSet<String>>,
        set_required: &mut HashSet<String>,
        name: &str,
        set: HashSet<Q>,
    ) {
        if !set.is_empty() {
            set_required.insert(name.to_string());
            for key in set.into_iter() {
                map.entry(key).or_default().insert(name.to_string());
            }
        }
    }

    fn get<'a, Q: Hash + Eq>(map: &'a HashMap<Q, HashSet<String>>, key: &Q) -> HashSet<&'a str> {
        map.get(key)
            .map(|set| set.iter().map(|name| name.as_str()).collect::<HashSet<_>>())
            .unwrap_or_default()
    }

    pub fn match_account(&self, pubkey: &Pubkey) -> HashSet<&str> {
        Self::get(&self.account, pubkey)
    }

    pub fn match_owner(&self, pubkey: &Pubkey) -> HashSet<&str> {
        Self::get(&self.owner, pubkey)
    }

    pub fn match_data_size(&self, data_size: usize) -> HashSet<&str> {
        Self::get(&self.data_size, &data_size)
    }

    // Any Tokenkeg Account
    pub fn match_tokenkeg(&self, account: &ReplicaAccountInfo) -> bool {
        account.owner == spl_token::ID
            && account.data.len() == SplTokenAccount::LEN
            && (!self.tokenkeg_owner.is_empty() || !self.tokenkeg_delegate.is_empty())
    }

    pub fn match_tokenkeg_owner(&self, pubkey: &Pubkey) -> HashSet<&str> {
        Self::get(&self.tokenkeg_owner, pubkey)
    }

    pub fn match_tokenkeg_delegate(&self, pubkey: &Pubkey) -> HashSet<&str> {
        Self::get(&self.tokenkeg_delegate, pubkey)
    }

    pub fn create_match(&self) -> AccountsFilterMatch {
        AccountsFilterMatch {
            accounts_filter: self,
            account: HashSet::new(),
            owner: HashSet::new(),
            data_size: HashSet::new(),
            tokenkeg_owner: HashSet::new(),
            tokenkeg_delegate: HashSet::new(),
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
pub struct TransactionsFilter {
    filter: ConfigTransactionsFilter,
}

impl TransactionsFilter {
    pub fn new(filter: ConfigTransactionsFilter) -> Self {
        Self { filter }
    }

    pub fn contains(&self, transaction: &ReplicaTransactionInfo) -> bool {
        if !self.filter.enabled {
            return false;
        }

        if transaction.is_vote && !self.filter.vote {
            return false;
        }

        if transaction.meta.err.is_some() && !self.filter.failed {
            return false;
        }

        if !self.contains_program(transaction) {
            return false;
        }

        true
    }

    fn contains_program(&self, transaction: &ReplicaTransactionInfo) -> bool {
        let ConfigTransactionsAccountsFilter { include, exclude } = &self.filter.accounts;
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
