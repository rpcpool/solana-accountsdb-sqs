use {
    super::{
        config::{ConfigAccountsFilter, ConfigTransactionsFilter},
        sqs::{ReplicaAccountInfo, ReplicaTransactionInfo},
    },
    solana_sdk::{program_pack::Pack, pubkey::Pubkey},
    spl_token::state::Account as SplTokenAccount,
};

#[derive(Debug, Default, Clone)]
pub struct AccountsFilter {
    filter: ConfigAccountsFilter,
}

impl AccountsFilter {
    pub fn new(filter: ConfigAccountsFilter) -> Self {
        Self { filter }
    }

    pub fn contains_owner_data_size(&self, account: &ReplicaAccountInfo) -> bool {
        // Filter by size only
        let data_size = account.data.len();
        if self.filter.data_size.contains(&data_size) {
            return true;
        }

        // Filter by owner with optional data size
        if let Some(entry) = self.filter.owner.get(&account.owner) {
            return entry.without_size || entry.sizes.contains(&data_size);
        }

        false
    }

    pub fn contains_tokenkeg(&self, account: &ReplicaAccountInfo) -> bool {
        // Any Tokenkeg Account
        account.owner == spl_token::ID
            && account.data.len() == SplTokenAccount::LEN
            && (!self.filter.tokenkeg_owner.is_empty() || !self.filter.tokenkeg_delegate.is_empty())
    }

    pub fn contains_tokenkeg_owner(&self, pubkey: &Pubkey) -> bool {
        self.filter.tokenkeg_owner.contains(pubkey)
    }

    pub fn contains_tokenkeg_delegate(&self, pubkey: &Pubkey) -> bool {
        self.filter.tokenkeg_delegate.contains(pubkey)
    }
}

#[derive(Debug, Default, Clone)]
pub struct TransactionsFilter {
    filter: ConfigTransactionsFilter,
}

impl TransactionsFilter {
    pub fn new(filter: ConfigTransactionsFilter) -> Self {
        Self { filter }
    }

    pub fn contains(&self, transaction: &ReplicaTransactionInfo) -> bool {
        if !self.filter.active {
            return false;
        }

        if transaction.is_vote && !self.filter.vote {
            return false;
        }

        true
    }
}
