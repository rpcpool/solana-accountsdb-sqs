use {
    crate::{
        config::Config,
        prom::PrometheusService,
        sqs::{AwsSqsClient, SqsClientResult},
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    tokio::{runtime::Runtime, time::Duration},
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    client: AwsSqsClient,
    prometheus: PrometheusService,
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_client<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&AwsSqsClient) -> SqsClientResult,
    {
        let inner = self.inner.as_ref().expect("initialized");
        f(&inner.client).map_err(|error| GeyserPluginError::Custom(Box::new(error)))
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        "GeyserPluginSqs"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;

        // Setup logger
        solana_logger::setup_with_default(&config.log.level);

        // Create inner
        let runtime = Runtime::new().map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;
        let prometheus = PrometheusService::new(&runtime, config.prometheus);
        let client = runtime
            .block_on(AwsSqsClient::new(config))
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        self.inner = Some(PluginInner {
            runtime,
            client,
            prometheus,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.prometheus.shutdown();
            inner.runtime.spawn(inner.client.shutdown());
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        _is_startup: bool,
    ) -> PluginResult<()> {
        self.with_client(|sqs| sqs.update_account(account, slot))
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        self.with_client(|sqs| sqs.startup_finished())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        self.with_client(|sqs| sqs.update_slot(slot, status))
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_client(|sqs| sqs.notify_transaction(transaction, slot))
    }

    fn notify_block_metadata(&mut self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        self.with_client(|sqs| sqs.notify_block_metadata(blockinfo))
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
