use {
    super::config::ConfigAccountsFilter, arrayref::array_ref,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::ReplicaAccountInfoVersions,
    solana_sdk::pubkey::Pubkey,
};

#[derive(Debug, Default)]
pub struct AccountsFilter {
    filters: Vec<ConfigAccountsFilter>,
}

impl AccountsFilter {
    pub fn new(filters: Vec<ConfigAccountsFilter>) -> Self {
        Self { filters }
    }

    pub fn contains(&self, account: &ReplicaAccountInfoVersions) -> bool {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                let owner = Pubkey::new_from_array(*array_ref!(account.owner, 0, 32));

                self.filters.iter().any(|filter| {
                    filter.owner.as_ref().map(|pk| owner == *pk).unwrap_or(true)
                        && filter
                            .data_size
                            .as_ref()
                            .map(|size| account.data.len() == *size)
                            .unwrap_or(true)
                })
            }
        }
    }
}
