use crate::sqs::SqsClientResult;

use {
    crate::{config::Config, sqs::AwsSqsClient},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
};

#[derive(Debug, Default)]
pub struct Plugin {
    sqs: Option<AwsSqsClient>,
}

impl Plugin {
    fn with_sqs<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&AwsSqsClient) -> SqsClientResult,
    {
        let sqs = self.sqs.as_ref().expect("initialized");
        f(sqs).map_err(|error| GeyserPluginError::Custom(Box::new(error)))
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        "GeyserPluginSqs"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        let log_level = config.log.level.as_deref().unwrap_or("info");
        solana_logger::setup_with_default(log_level);

        // Sqs client
        self.sqs = Some(
            AwsSqsClient::new(config)
                .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?,
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
        _is_startup: bool,
    ) -> PluginResult<()> {
        self.with_sqs(|sqs| sqs.update_account(account, slot))
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        self.with_sqs(|sqs| sqs.startup_finished())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        self.with_sqs(|sqs| sqs.update_slot(slot, status))
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_sqs(|sqs| sqs.notify_transaction(transaction, slot))
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
