use {
    crate::{config::Config, filter::AccountsFilter, sqs::AwsSqsClient},
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPlugin, AccountsDbPluginError, ReplicaAccountInfoVersions,
        Result as PluginResult, SlotStatus,
    },
};

#[derive(Debug, Default)]
pub struct Plugin {
    filter: AccountsFilter,
    sqs: Option<AwsSqsClient>,
}

impl Plugin {
    fn get_sqs(&self) -> &AwsSqsClient {
        self.sqs.as_ref().expect("initialized")
    }
}

impl AccountsDbPlugin for Plugin {
    fn name(&self) -> &'static str {
        "AccountsDbPluginTokens"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        let log_level = config.log.level.as_deref().unwrap_or("info");
        solana_logger::setup_with_default(log_level);

        // Accounts filter
        self.filter = AccountsFilter::new(config.filters);

        // Sqs client
        self.sqs = Some(
            AwsSqsClient::new(config.sqs)
                .map_err(|error| AccountsDbPluginError::Custom(Box::new(error)))?,
        );

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(sqs) = self.sqs.take() {
            sqs.shutdown();
        }
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        if !is_startup && self.filter.contains(&account) {
            self.get_sqs()
                .update_account((account, slot).into())
                .map_err(|error| AccountsDbPluginError::Custom(Box::new(error)))?
        }
        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &mut self,
        _slot: u64,
        _parent: Option<u64>,
        _status: SlotStatus,
    ) -> PluginResult<()> {
        Ok(())
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait AccountsDbPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn AccountsDbPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn AccountsDbPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
