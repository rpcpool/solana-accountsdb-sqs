use {
    super::{
        config::{Config, ConfigAwsAuth, ConfigAwsSqs},
        filter::AccountsFilter,
    },
    arrayref::array_ref,
    async_trait::async_trait,
    futures::future::FutureExt,
    humantime::format_duration,
    log::*,
    rusoto_core::{HttpClient, RusotoError},
    rusoto_credential::{
        AutoRefreshingProvider, AwsCredentials, CredentialsError, ProfileProvider,
        ProvideAwsCredentials, StaticProvider,
    },
    rusoto_sqs::{
        GetQueueAttributesError, GetQueueAttributesRequest, SendMessageBatchError,
        SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs, SqsClient as RusotoSqsClient,
    },
    serde::{Deserialize, Serialize, Serializer},
    serde_json::json,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoVersions, SlotStatus as GeyserSlotStatus,
    },
    solana_sdk::pubkey::Pubkey,
    spl_token::{solana_program::program_pack::Pack, state::Account as SplTokenAccount},
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        convert::TryInto,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::{
        runtime::Runtime,
        sync::{mpsc, Semaphore},
        time::sleep as sleep_async,
    },
};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ReplicaAccountInfo {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub write_version: u64,
    pub slot: u64,
}

impl ReplicaAccountInfo {
    pub fn token_owner(&self) -> Option<Pubkey> {
        if self.owner == spl_token::ID && self.data.len() == SplTokenAccount::LEN {
            Some(Pubkey::new_from_array(*array_ref!(&self.data, 32, 32)))
        } else {
            None
        }
    }

    pub fn token_delegate(&self) -> Option<Option<Pubkey>> {
        if self.owner == spl_token::ID && self.data.len() == SplTokenAccount::LEN {
            Some(match *array_ref!(&self.data, 72, 4) {
                [0, 0, 0, 0] => None,
                [1, 0, 0, 0] => {
                    let pubkey = Pubkey::new_from_array(*array_ref!(&self.data, 76, 32));
                    Some(pubkey)
                }
                _ => None,
            })
        } else {
            None
        }
    }
}

impl PartialOrd for ReplicaAccountInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplicaAccountInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.slot.cmp(&other.slot) {
            std::cmp::Ordering::Equal => self.write_version.cmp(&other.write_version),
            other => other,
        }
    }
}

impl<'a> From<(ReplicaAccountInfoVersions<'a>, u64)> for ReplicaAccountInfo {
    fn from((account, slot): (ReplicaAccountInfoVersions<'a>, u64)) -> Self {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => Self {
                pubkey: Pubkey::new(account.pubkey),
                lamports: account.lamports,
                owner: Pubkey::new(account.owner),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.into(),
                write_version: account.write_version,
                slot,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
}

impl Default for SlotStatus {
    fn default() -> Self {
        SlotStatus::Finalized
    }
}

impl From<GeyserSlotStatus> for SlotStatus {
    fn from(status: GeyserSlotStatus) -> Self {
        match status {
            GeyserSlotStatus::Processed => Self::Processed,
            GeyserSlotStatus::Confirmed => Self::Confirmed,
            GeyserSlotStatus::Rooted => Self::Finalized,
        }
    }
}

#[derive(Debug)]
enum Message {
    UpdateAccount(ReplicaAccountInfo),
    UpdateSlot((SlotStatus, u64)),
    StartupFinished,
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
enum FilterType {
    OwnerDataSize,
    TokenkegOwner,
    TokenkegDelegate,
}

impl Serialize for FilterType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match *self {
            FilterType::OwnerDataSize => "owner_data_size",
            FilterType::TokenkegOwner => "tokenkeg_owner",
            FilterType::TokenkegDelegate => "tokenkeg_delegate",
        })
    }
}

#[derive(Debug)]
enum SendMessage {
    Account((ReplicaAccountInfo, Vec<FilterType>)),
    Slot((SlotStatus, u64)),
}

#[derive(Debug, Error)]
pub enum SqsClientError {
    #[error("invalid commitment_level: {0:?}")]
    InvalidCommitmentLevel(SlotStatus),
    #[error("failed to create Runtime: {0}")]
    RuntimeCreate(std::io::Error),
    #[error("aws credential error: {0}")]
    AwsCredentials(#[from] CredentialsError),
    #[error("HttpClient error: {0}")]
    AwsHttpClientTls(#[from] rusoto_core::request::TlsError),
    #[error("failed to get queue attributes: {0}")]
    SqsGetAttributes(#[from] RusotoError<GetQueueAttributesError>),
    #[error("failed to send messages: {0}")]
    SqsSendMessageBatch(#[from] RusotoError<SendMessageBatchError>),
    #[error("failed to send some messages: {0}")]
    SqsSendMessageBatchEntry(String),
    #[error("send message through send queue failed: channel is closed")]
    UpdateQueueChannelClosed,
}

pub type SqsClientResult<T = ()> = Result<T, SqsClientError>;

#[derive(Debug)]
pub struct AwsSqsClient {
    runtime: Runtime,
    send_queue: mpsc::UnboundedSender<Message>,
    startup_job: Arc<AtomicBool>,
    send_job: Arc<AtomicBool>,
}

impl AwsSqsClient {
    pub fn new(config: Config) -> SqsClientResult<Self> {
        if !matches!(
            config.sqs.commitment_level,
            SlotStatus::Confirmed | SlotStatus::Finalized
        ) {
            return Err(SqsClientError::InvalidCommitmentLevel(
                config.sqs.commitment_level,
            ));
        }

        let startup_job = Arc::new(AtomicBool::new(true));
        let startup_job_loop = Arc::clone(&startup_job);
        let send_job = Arc::new(AtomicBool::new(true));
        let send_job_loop = Arc::clone(&send_job);
        let runtime = Runtime::new().map_err(SqsClientError::RuntimeCreate)?;
        let send_queue = runtime.block_on(async move {
            // Check that SQS is available
            let (client, queue_url) = Self::create_sqs(config.sqs.clone())?;
            client
                .get_queue_attributes(GetQueueAttributesRequest {
                    attribute_names: None,
                    queue_url: queue_url.clone(),
                })
                .await?;

            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                if let Err(error) =
                    Self::send_loop(config, rx, startup_job_loop, send_job_loop).await
                {
                    error!("update_loop failed: {:?}", error);
                }
            });

            Ok::<_, SqsClientError>(tx)
        })?;

        Ok(Self {
            runtime,
            send_queue,
            startup_job,
            send_job,
        })
    }

    pub fn create_sqs(config: ConfigAwsSqs) -> SqsClientResult<(RusotoSqsClient, String)> {
        let request_dispatcher = HttpClient::new()?;
        let credentials_provider = AwsCredentialsProvider::new(config.auth)?;
        let sqs =
            RusotoSqsClient::new_with(request_dispatcher, credentials_provider, config.region);
        Ok((sqs, config.url))
    }

    fn send_message(&self, message: Message) -> SqsClientResult {
        self.send_queue
            .send(message)
            .map_err(|_| SqsClientError::UpdateQueueChannelClosed)
    }

    pub fn shutdown(self) {
        if self.send_message(Message::Shutdown).is_ok() {
            while self.send_job.load(Ordering::Relaxed) {
                sleep(Duration::from_micros(10));
            }
        }

        self.runtime.shutdown_timeout(Duration::from_secs(10));
    }

    pub fn update_slot(&self, slot: u64, status: GeyserSlotStatus) -> SqsClientResult {
        self.send_message(Message::UpdateSlot((status.into(), slot)))
    }

    pub fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
    ) -> SqsClientResult {
        self.send_message(Message::UpdateAccount((account, slot).into()))
    }

    pub fn startup_finished(&self) -> SqsClientResult {
        self.send_message(Message::StartupFinished)?;
        while self.startup_job.load(Ordering::Relaxed) {
            sleep(Duration::from_micros(10));
        }
        Ok(())
    }

    async fn send_loop(
        config: Config,
        mut rx: mpsc::UnboundedReceiver<Message>,
        startup_job: Arc<AtomicBool>,
        send_job: Arc<AtomicBool>,
    ) -> SqsClientResult {
        let max_requests = config.sqs.max_requests;
        let commitment_level = config.sqs.commitment_level;
        let (client, queue_url) = Self::create_sqs(config.sqs)?;
        let is_slot_messages_enabled = config.slots.messages;
        let filter = AccountsFilter::new(config.filter);

        // Save required Tokenkeg Accounts
        let mut tokenkeg_owner_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        let mut tokenkeg_delegate_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        while let Some(message) = rx.recv().await {
            match message {
                Message::UpdateAccount(account) => {
                    if let Some(owner) = account.token_owner() {
                        if filter.contains_tokenkeg_owner(&owner) {
                            tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                        }
                    }
                    if let Some(Some(delegate)) = account.token_delegate() {
                        if filter.contains_tokenkeg_delegate(&delegate) {
                            tokenkeg_delegate_accounts.insert(account.pubkey, account.clone());
                        }
                    }
                }
                Message::UpdateSlot(_) => unreachable!(),
                Message::StartupFinished => {
                    startup_job.store(false, Ordering::Relaxed);
                    break;
                }
                Message::Shutdown => {
                    send_job.store(false, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
        info!("startup finished");

        // Spawn simple stats
        let send_job_stats = Arc::clone(&send_job);
        let (stats_queued_tx, mut stats_queued_rx) = mpsc::unbounded_channel();
        let (stats_inprocess_tx, mut stats_inprocess_rx) = mpsc::unbounded_channel();
        let (stats_processed_tx, mut stats_processed_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut last_update = Instant::now();
            let mut queued = 0;
            let mut inprocess = 0;
            let mut processed = 0;
            let mut processed_time = Duration::from_secs(0);
            loop {
                if !send_job_stats.load(Ordering::Relaxed) {
                    break;
                }

                while let Ok(count) = stats_queued_rx.try_recv() {
                    queued += count;
                }
                while let Ok(count) = stats_inprocess_rx.try_recv() {
                    queued -= count;
                    inprocess += count;
                }
                while let Ok((count, elapsed)) = stats_processed_rx.try_recv() {
                    inprocess -= count;
                    processed += count;
                    processed_time += elapsed * count as u32;
                }

                if last_update.elapsed() < Duration::from_secs(10) {
                    sleep_async(Duration::from_micros(1_000)).await;
                    continue;
                }

                log!(
                    if inprocess > 200 {
                        Level::Warn
                    } else {
                        Level::Info
                    },
                    "queued: {}, in process: {}, processed: {}, avg processing time: {}",
                    queued,
                    inprocess,
                    processed,
                    format_duration(
                        processed_time
                            .checked_div(processed as u32)
                            .unwrap_or_default()
                    )
                );

                last_update = Instant::now();
                processed = 0;
                processed_time = Duration::from_secs(0);
            }
        });

        // Messages, accounts and tokenkeg history changes
        let mut messages = vec![];
        let mut accounts: BTreeMap<u64, BTreeSet<ReplicaAccountInfo>> = BTreeMap::new();
        type TokenkegHist = BTreeMap<u64, HashMap<Pubkey, Option<ReplicaAccountInfo>>>;
        let mut tokenkeg_owner_accounts_hist: TokenkegHist = BTreeMap::new();
        let mut tokenkeg_delegate_accounts_hist: TokenkegHist = BTreeMap::new();

        // Remove outdated slots from `BTreeMap<u64, T>` (accounts, tokenkeg history)
        fn remove_outdated_slots<T>(map: &mut BTreeMap<u64, T>, min_slot: u64) {
            loop {
                match map.keys().next().cloned() {
                    Some(slot) if slot < min_slot => map.remove(&slot),
                    _ => break,
                };
            }
        }

        // Add new messages for an accounts on commitment_level
        fn generate_messages(
            messages: &mut Vec<SendMessage>,
            accounts: &BTreeSet<ReplicaAccountInfo>,
            filter: &AccountsFilter,
            tokenkeg_owner_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_delegate_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_owner_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
            tokenkeg_delegate_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
        ) {
            for account in accounts {
                let mut filters = Vec::with_capacity(3);

                if filter.contains_owner_data_size(account) {
                    filters.push(FilterType::OwnerDataSize);
                }

                if filter.contains_tokenkeg(account) {
                    let owner = account.token_owner().expect("valid tokenkeg");
                    if let Some((pubkey, value)) =
                        match tokenkeg_owner_accounts.get(&account.pubkey) {
                            Some(existed)
                                if existed.token_owner().expect("valid tokenkeg") != owner =>
                            {
                                let prev = if filter.contains_tokenkeg_owner(&owner) {
                                    tokenkeg_owner_accounts.insert(account.pubkey, account.clone())
                                } else {
                                    tokenkeg_owner_accounts.remove(&account.pubkey)
                                };
                                Some((account.pubkey, prev))
                            }
                            None if filter.contains_tokenkeg_owner(&owner) => {
                                tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                                Some((account.pubkey, None))
                            }
                            _ => None,
                        }
                    {
                        tokenkeg_owner_accounts_hist.entry(pubkey).or_insert(value);
                        filters.push(FilterType::TokenkegOwner);
                    }

                    let delegate = account.token_delegate().expect("valid tokenkeg");
                    if let Some((pubkey, value)) = match (
                        delegate,
                        delegate.and_then(|_| tokenkeg_delegate_accounts.get(&account.pubkey)),
                    ) {
                        (Some(delegate), Some(existed))
                            if existed
                                .token_delegate()
                                .expect("valid tokenkeg")
                                .expect("valid delegate")
                                != delegate =>
                        {
                            let prev = if filter.contains_tokenkeg_delegate(&delegate) {
                                tokenkeg_delegate_accounts.insert(account.pubkey, account.clone())
                            } else {
                                tokenkeg_delegate_accounts.remove(&account.pubkey)
                            };
                            Some((account.pubkey, prev))
                        }
                        (Some(delegate), None) if filter.contains_tokenkeg_delegate(&delegate) => {
                            tokenkeg_delegate_accounts.insert(account.pubkey, account.clone());
                            Some((account.pubkey, None))
                        }
                        _ => None,
                    } {
                        tokenkeg_delegate_accounts_hist
                            .entry(pubkey)
                            .or_insert(value);
                        filters.push(FilterType::TokenkegDelegate);
                    }
                }

                if !filters.is_empty() {
                    messages.push(SendMessage::Account((account.clone(), filters)));
                }
            }
        }

        // Restore previous state in case of fork
        fn tokenkeg_revert(
            accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            hist: HashMap<Pubkey, Option<ReplicaAccountInfo>>,
        ) {
            for (pubkey, maybe_account) in hist.into_iter() {
                match maybe_account {
                    Some(account) => accounts.insert(pubkey, account),
                    None => accounts.remove(&pubkey),
                };
            }
        }

        // Handle messages
        let send_jobs = Arc::new(Semaphore::new(max_requests));
        let mut account_current_slot = 0;
        let mut status_current_slot = 0;
        loop {
            if !messages.is_empty() && send_jobs.available_permits() > 0 {
                let slice = 0..messages.len().min(10);
                let messages = messages.drain(slice).collect::<Vec<_>>();
                let messages_count = messages.len();
                let _ = stats_inprocess_tx.send(messages_count);
                let client = client.clone();
                let queue_url = queue_url.clone();
                let stats_processed_tx = stats_processed_tx.clone();
                let send_jobs = Arc::clone(&send_jobs);
                let send_permit = send_jobs.try_acquire_owned().expect("available permit");
                tokio::spawn(async move {
                    let ts = Instant::now();
                    if let Err(error) = Self::send_messages(client, queue_url, messages).await {
                        error!("failed to send data: {:?}", error);
                    }
                    let _ = stats_processed_tx.send((messages_count, ts.elapsed()));
                    drop(send_permit);
                });
                continue;
            }

            let send_jobs_readiness = if messages.is_empty() {
                futures::future::pending().boxed()
            } else {
                send_jobs.acquire().map(|_| ()).boxed()
            };

            tokio::select! {
                _ = send_jobs_readiness => {},
                message = rx.recv() => match message {
                    Some(Message::UpdateAccount(account)) => {
                        if account_current_slot != account.slot {
                            // Drop previous accounts changes
                            accounts.insert(account.slot, Default::default());
                            account_current_slot = account.slot;
                        }

                        accounts
                            .get_mut(&account.slot)
                            .expect("already inserted")
                            .insert(account);
                    }
                    Some(Message::UpdateSlot((status, slot))) => {
                        if is_slot_messages_enabled {
                            messages.push(SendMessage::Slot((status, slot)));
                            let _ = stats_queued_tx.send(1);
                        }

                        if status == commitment_level {
                            assert!(matches!(
                                commitment_level,
                                SlotStatus::Confirmed | SlotStatus::Finalized
                            ));

                            // Remove outdated data (keep 320 slots)
                            if let Some(min_slot) = slot.checked_sub(320) {
                                remove_outdated_slots(&mut accounts, min_slot);
                                remove_outdated_slots(&mut tokenkeg_owner_accounts_hist, min_slot);
                                remove_outdated_slots(&mut tokenkeg_delegate_accounts_hist, min_slot);
                            }

                            // Handle reorg
                            for slot in (slot..=status_current_slot).rev() {
                                if let Some(hist) = tokenkeg_owner_accounts_hist.remove(&slot) {
                                    tokenkeg_revert(&mut tokenkeg_owner_accounts, hist);
                                }
                                if let Some(hist) = tokenkeg_delegate_accounts_hist.remove(&slot) {
                                    tokenkeg_revert(&mut tokenkeg_delegate_accounts, hist);
                                }
                            }

                            match accounts.get(&slot) {
                                Some(accounts) => {
                                    let size = messages.len();
                                    generate_messages(
                                        &mut messages,
                                        accounts,
                                        &filter,
                                        &mut tokenkeg_owner_accounts,
                                        &mut tokenkeg_delegate_accounts,
                                        tokenkeg_owner_accounts_hist.entry(slot).or_default(),
                                        tokenkeg_delegate_accounts_hist.entry(slot).or_default(),
                                    );
                                    let _ = stats_queued_tx.send(messages.len() - size);
                                }
                                None => error!(
                                    "send_loop error: accounts for slot {} does not exists",
                                    slot
                                ),
                            }

                            status_current_slot = slot;
                        }
                    }
                    Some(Message::StartupFinished) => unreachable!(),
                    Some(Message::Shutdown) | None => break,
                }
            }
        }
        let _ = send_jobs
            .acquire_many(max_requests.try_into().expect("valid size"))
            .await
            .expect("alive");
        send_job.store(false, Ordering::Relaxed);
        info!("update_loop finished");

        Ok(())
    }

    async fn send_messages(
        client: RusotoSqsClient,
        queue_url: String,
        messages: Vec<SendMessage>,
    ) -> SqsClientResult {
        let entries = messages
            .iter()
            .enumerate()
            .map(|(id, message)| SendMessageBatchRequestEntry {
                id: id.to_string(),
                message_body: match message {
                    SendMessage::Account((account, filters)) => json!({
                        "type": "account",
                        "filters": filters,
                        "pubkey": account.pubkey.to_string(),
                        "lamports": account.lamports,
                        "owner": account.owner.to_string(),
                        "executable": account.executable,
                        "rent_epoch": account.rent_epoch,
                        "data": base64::encode(&account.data),
                        "write_version": account.write_version,
                        "slot": account.slot,
                    }),
                    SendMessage::Slot((status, slot)) => json!({
                        "type": "slot",
                        "status": status,
                        "slot": slot,
                    }),
                }
                .to_string(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let failed = client
            .send_message_batch(SendMessageBatchRequest { entries, queue_url })
            .await?
            .failed;

        if failed.is_empty() {
            Ok(())
        } else {
            Err(SqsClientError::SqsSendMessageBatchEntry(
                serde_json::to_string(
                    &failed
                        .into_iter()
                        .map(|entry| {
                            let mut value = json!({
                                "code": entry.code,
                                "message": entry.message,
                                "sender_fault": entry.sender_fault,
                            });

                            let index = entry.id.parse::<usize>().ok();
                            match index.and_then(|index| messages.get(index)).unwrap() {
                                SendMessage::Account((account, _filters)) => {
                                    value["pubkey"] = json!(account.pubkey.to_string());
                                    value["slot"] = json!(account.slot);
                                    value["write_version"] = json!(account.write_version);
                                }
                                SendMessage::Slot((status, slot)) => {
                                    value["status"] = json!(status);
                                    value["slot"] = json!(slot);
                                }
                            }

                            value
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            ))
        }
    }
}

enum AwsCredentialsProvider {
    Static(StaticProvider),
    File(AutoRefreshingProvider<ProfileProvider>),
}

impl AwsCredentialsProvider {
    fn new(config: ConfigAwsAuth) -> SqsClientResult<Self> {
        match config {
            ConfigAwsAuth::Static {
                access_key_id,
                secret_access_key,
            } => Ok(Self::Static(StaticProvider::new_minimal(
                access_key_id,
                secret_access_key,
            ))),
            ConfigAwsAuth::File {
                credentials_file,
                profile,
            } => Ok(Self::File(AutoRefreshingProvider::new(
                ProfileProvider::with_configuration(
                    credentials_file,
                    profile.unwrap_or_else(|| "default".to_owned()),
                ),
            )?)),
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for AwsCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        match self {
            Self::Static(p) => p.credentials(),
            Self::File(p) => p.credentials(),
        }
        .await
    }
}
