use {
    super::config::ConfigAccountsFilter, arrayref::array_ref,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::ReplicaAccountInfoVersions,
    solana_sdk::pubkey::Pubkey, std::collections::HashSet,
};

#[derive(Debug, Default)]
pub struct AccountsFilter {
    owners: HashSet<Pubkey>,
}

impl AccountsFilter {
    pub fn new(config: &[ConfigAccountsFilter]) -> Self {
        let mut filter = Self::default();

        for item in config {
            if let Some(pubkey) = item.owner {
                filter.owners.insert(pubkey);
            }
        }

        filter
    }

    pub fn contains(&self, account: &ReplicaAccountInfoVersions) -> bool {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let owner = Pubkey::new_from_array(*array_ref!(account.owner, 0, 32));
                self.owners.contains(&owner)
            }
        }
    }
}
