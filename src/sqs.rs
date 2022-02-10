use {
    super::{
        config::{Config, ConfigAwsAuth, ConfigAwsSqs},
        filter::AccountsFilter,
    },
    arrayref::array_ref,
    async_trait::async_trait,
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
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        ReplicaAccountInfoVersions, SlotStatus as AccountsDbSlotStatus,
    },
    solana_sdk::pubkey::Pubkey,
    spl_token::{solana_program::program_pack::Pack, state::Account as SplTokenAccount},
    std::{
        collections::{
            hash_map::Entry as HashMapEntry, BTreeMap, BTreeSet, HashMap, HashSet, LinkedList,
        },
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::{runtime::Runtime, sync::mpsc, time::sleep as sleep_async},
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

impl From<AccountsDbSlotStatus> for SlotStatus {
    fn from(status: AccountsDbSlotStatus) -> Self {
        match status {
            AccountsDbSlotStatus::Processed => Self::Processed,
            AccountsDbSlotStatus::Confirmed => Self::Confirmed,
            AccountsDbSlotStatus::Rooted => Self::Finalized,
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
    Account((Vec<FilterType>, ReplicaAccountInfo)),
    Slot((SlotStatus, u64)),
}

#[derive(Debug, Error)]
pub enum SqsClientError {
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
    filter: AccountsFilter,
    send_queue: mpsc::UnboundedSender<Message>,
    send_jobs: Arc<AtomicU64>,
    startup_job: Arc<AtomicBool>,
}

impl AwsSqsClient {
    pub fn new(config: Config) -> SqsClientResult<Self> {
        let filter = AccountsFilter::new(config.filter.clone());
        let send_jobs = Arc::new(AtomicU64::new(1));
        let send_jobs_loop = Arc::clone(&send_jobs);
        let startup_job = Arc::new(AtomicBool::new(true));
        let startup_job_loop = Arc::clone(&startup_job);
        let runtime = Runtime::new().map_err(SqsClientError::RuntimeCreate)?;
        let send_queue = runtime.block_on(async move {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                if let Err(error) =
                    Self::send_loop(config, rx, send_jobs_loop, startup_job_loop).await
                {
                    error!("update_loop failed: {:?}", error);
                }
            });

            Ok::<_, SqsClientError>(tx)
        })?;
        Ok(Self {
            runtime,
            filter,
            send_queue,
            send_jobs,
            startup_job,
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
        if let Ok(()) = self.send_message(Message::Shutdown) {
            while self.send_jobs.load(Ordering::Relaxed) > 0 {
                sleep(Duration::from_micros(10));
            }
        }

        self.runtime.shutdown_timeout(Duration::from_secs(10));
    }

    pub fn update_slot(&self, slot: u64, status: AccountsDbSlotStatus) -> SqsClientResult {
        self.send_message(Message::UpdateSlot((status.into(), slot)))
    }

    pub fn update_account(&self, account: ReplicaAccountInfo) -> SqsClientResult {
        if self.filter.contains_owner_data_size(&account) || self.filter.contains_tokenkeg(&account)
        {
            self.send_message(Message::UpdateAccount(account))?;
        }
        Ok(())
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
        send_jobs: Arc<AtomicU64>,
        startup_job: Arc<AtomicBool>,
    ) -> SqsClientResult {
        let max_requests = config.sqs.max_requests;
        let commitment_level = config.sqs.commitment_level;
        let (client, queue_url) = Self::create_sqs(config.sqs)?;
        let is_slot_messages_enabled = config.slots.messages;
        let filter = AccountsFilter::new(config.filter);

        // Check that SQS is available
        client
            .get_queue_attributes(GetQueueAttributesRequest {
                attribute_names: None,
                queue_url: queue_url.clone(),
            })
            .await?;

        // Save required Tokenkeg Accounts
        let mut tokenkeg_owner_accounts: HashMap<Pubkey, BTreeSet<ReplicaAccountInfo>> =
            HashMap::new();
        let mut tokenkeg_delegate_accounts: HashMap<Pubkey, BTreeSet<ReplicaAccountInfo>> =
            HashMap::new();
        while let Some(message) = rx.recv().await {
            match message {
                Message::UpdateAccount(account) => {
                    if let Some(owner) = account.token_owner() {
                        if filter.contains_tokenkeg_owner(&owner) {
                            tokenkeg_owner_accounts
                                .entry(account.pubkey)
                                .or_default()
                                .insert(account.clone());
                        }
                    }
                    if let Some(Some(delegate)) = account.token_delegate() {
                        if filter.contains_tokenkeg_delegate(&delegate) {
                            tokenkeg_delegate_accounts
                                .entry(account.pubkey)
                                .or_default()
                                .insert(account);
                        }
                    }
                }
                Message::UpdateSlot(_) => unreachable!(),
                Message::StartupFinished => {
                    startup_job.store(false, Ordering::Relaxed);
                    break;
                }
                Message::Shutdown => {
                    send_jobs.fetch_sub(1, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }

        // spawn stats
        let send_jobs_stats = Arc::clone(&send_jobs);
        let (stats_queued_tx, mut stats_queued_rx) = mpsc::unbounded_channel();
        let (stats_processed_tx, mut stats_processed_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut last_update = Instant::now();
            let mut queued = 0;
            let mut processed = 0;
            let mut processed_time = Duration::from_secs(0);
            let mut inprocess = 0;
            loop {
                if send_jobs_stats.load(Ordering::Relaxed) == 0 {
                    break;
                }

                while let Ok(()) = stats_queued_rx.try_recv() {
                    queued += 1;
                }
                while let Ok(elapsed) = stats_processed_rx.try_recv() {
                    processed += 1;
                    processed_time += elapsed;
                }

                if last_update.elapsed() < Duration::from_secs(10) {
                    sleep_async(Duration::from_micros(1_000)).await;
                    continue;
                }

                inprocess += queued - processed;
                log!(
                    if inprocess > 200 {
                        Level::Warn
                    } else {
                        Level::Info
                    },
                    "queued: {}, processed: {}, in process: {}, avg processing time: {}",
                    queued,
                    processed,
                    inprocess,
                    format_duration(
                        processed_time
                            .checked_div(processed as u32)
                            .unwrap_or_default()
                    )
                );

                last_update = Instant::now();
                queued = 0;
                processed = 0;
                processed_time = Duration::from_secs(0);
            }
        });

        let mut current_slot = match commitment_level {
            Some(_) => 0,
            None => u64::MAX,
        };
        let mut slots_messages: BTreeMap<u64, LinkedList<SendMessage>> = BTreeMap::new();
        fn compact_messages(
            slots_messages: &mut BTreeMap<u64, LinkedList<SendMessage>>,
            compact_slot: u64,
        ) {
            // Merge possible filters
            let mut vec = Vec::new();
            let mut accounts = HashMap::new();
            if let Some(list) = slots_messages.remove(&compact_slot) {
                for item in list {
                    match item {
                        SendMessage::Account((mut filters, account)) => {
                            match accounts.entry((account.pubkey, account.write_version)) {
                                HashMapEntry::Occupied(e) => {
                                    let index = *e.get();
                                    if let Some(SendMessage::Account((afilters, _))) =
                                        vec.get_mut(index)
                                    {
                                        afilters.append(&mut filters);
                                    }
                                }
                                HashMapEntry::Vacant(e) => {
                                    e.insert(vec.len());
                                    vec.push(SendMessage::Account((filters, account)));
                                }
                            }
                        }
                        SendMessage::Slot(v) => vec.push(SendMessage::Slot(v)),
                    }
                }
            }
            slots_messages.insert(compact_slot, vec.into_iter().collect());
        }
        fn fetch_slots_messages(
            current_slot: u64,
            slots_messages: &mut BTreeMap<u64, LinkedList<SendMessage>>,
            messages: &mut Vec<SendMessage>,
        ) {
            while messages.len() < messages.capacity() {
                let slot = match slots_messages.keys().next().copied() {
                    Some(slot) if slot <= current_slot => slot,
                    _ => break,
                };
                let mut list = slots_messages.remove(&slot).unwrap();
                while messages.len() < messages.capacity() {
                    match list.pop_front() {
                        Some(message) => messages.push(message),
                        None => break,
                    };
                }
                if !list.is_empty() {
                    slots_messages.insert(slot, list);
                }
            }
        }

        let mut tokenkeg_owner_waiting_accounts = Vec::new();
        let mut tokenkeg_owner_touched_accounts = HashSet::new();
        let mut tokenkeg_delegate_waiting_accounts = Vec::new();
        let mut tokenkeg_delegate_touched_accounts = HashSet::new();
        #[allow(clippy::too_many_arguments)]
        fn drain_tokenkeg_accounts(
            filter: &AccountsFilter,
            tokenkeg_owner_accounts: &mut HashMap<Pubkey, BTreeSet<ReplicaAccountInfo>>,
            tokenkeg_owner_waiting_accounts: &mut Vec<ReplicaAccountInfo>,
            tokenkeg_owner_touched_accounts: &mut HashSet<Pubkey>,
            tokenkeg_delegate_accounts: &mut HashMap<Pubkey, BTreeSet<ReplicaAccountInfo>>,
            tokenkeg_delegate_waiting_accounts: &mut Vec<ReplicaAccountInfo>,
            tokenkeg_delegate_touched_accounts: &mut HashSet<Pubkey>,
            slots_messages: &mut BTreeMap<u64, LinkedList<SendMessage>>,
        ) {
            let mut accounts = HashSet::new();
            for account in tokenkeg_owner_waiting_accounts.drain(..) {
                if let Some(set) = tokenkeg_owner_accounts.get_mut(&account.pubkey) {
                    tokenkeg_owner_touched_accounts.insert(account.pubkey);
                    set.insert(account);
                }
            }
            for pubkey in tokenkeg_owner_touched_accounts.drain() {
                let set = tokenkeg_owner_accounts
                    .get_mut(&pubkey)
                    .expect("invalid touched account");
                if set.len() == 1 {
                    continue;
                }

                let mut owner_prev = set.iter().next().unwrap().token_owner().unwrap();
                for account in set.iter().skip(1) {
                    let owner = account.token_owner().unwrap();
                    if owner != owner_prev
                        && (filter.contains_tokenkeg_owner(&owner)
                            || filter.contains_tokenkeg_owner(&owner_prev))
                    {
                        accounts.insert(account.clone());
                    }
                    owner_prev = owner;
                }

                let last = set.iter().last().unwrap().clone();
                let last_owner = last.token_owner().expect("token account");
                if filter.contains_tokenkeg_owner(&last_owner) {
                    set.clear();
                    set.insert(last);
                } else {
                    tokenkeg_owner_accounts.remove(&pubkey);
                }
            }
            for account in accounts.into_iter() {
                slots_messages
                    .entry(account.slot)
                    .or_default()
                    .push_back(SendMessage::Account((
                        vec![FilterType::TokenkegOwner],
                        account,
                    )));
            }

            let mut accounts = HashSet::new();
            for account in tokenkeg_delegate_waiting_accounts.drain(..) {
                if let Some(set) = tokenkeg_delegate_accounts.get_mut(&account.pubkey) {
                    tokenkeg_delegate_touched_accounts.insert(account.pubkey);
                    set.insert(account);
                }
            }
            for pubkey in tokenkeg_delegate_touched_accounts.drain() {
                let set = tokenkeg_delegate_accounts
                    .get_mut(&pubkey)
                    .expect("invalid touched account");
                if set.len() == 1 {
                    continue;
                }

                let mut delegate_prev = set.iter().next().unwrap().token_delegate().unwrap();
                for account in set.iter().skip(1) {
                    let delegate = account.token_delegate().unwrap();
                    if delegate != delegate_prev
                        && (delegate
                            .map(|pubkey| filter.contains_tokenkeg_delegate(&pubkey))
                            .unwrap_or(false)
                            || delegate_prev
                                .map(|pubkey| filter.contains_tokenkeg_delegate(&pubkey))
                                .unwrap_or(false))
                    {
                        accounts.insert(account.clone());
                    }
                    delegate_prev = delegate;
                }

                let last = set.iter().last().unwrap().clone();
                let last_delegate = last.token_delegate().expect("token account");
                if last_delegate
                    .map(|pubkey| filter.contains_tokenkeg_delegate(&pubkey))
                    .unwrap_or(false)
                {
                    set.clear();
                    set.insert(last);
                } else {
                    tokenkeg_owner_accounts.remove(&pubkey);
                }
            }
            for account in accounts.into_iter() {
                slots_messages
                    .entry(account.slot)
                    .or_default()
                    .push_back(SendMessage::Account((
                        vec![FilterType::TokenkegDelegate],
                        account,
                    )));
            }
        }

        'outer: loop {
            while send_jobs.load(Ordering::Relaxed) >= max_requests {
                // `tokio::time::sleep(Duration::from_nanos(100)).await` or some signal?
                tokio::task::yield_now().await;
            }

            let mut messages = Vec::with_capacity(10);
            fetch_slots_messages(current_slot, &mut slots_messages, &mut messages);
            while messages.len() < messages.capacity() {
                // Wait first message in the queue and fetch all available but not more than max
                let maybe_message = if messages.is_empty() {
                    rx.recv().await
                } else {
                    match rx.try_recv() {
                        Ok(message) => Some(message),
                        Err(mpsc::error::TryRecvError::Disconnected) => None,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                    }
                };
                match maybe_message {
                    Some(Message::UpdateAccount(account)) => {
                        if filter.contains_owner_data_size(&account) {
                            slots_messages.entry(account.slot).or_default().push_back(
                                SendMessage::Account((
                                    vec![FilterType::OwnerDataSize],
                                    account.clone(),
                                )),
                            );
                        }

                        if filter.contains_tokenkeg(&account) {
                            let owner = account.token_owner().expect("bad filter");
                            if filter.contains_tokenkeg_owner(&owner) {
                                tokenkeg_owner_touched_accounts.insert(account.pubkey);
                                let set = tokenkeg_owner_accounts.entry(account.pubkey);
                                if let HashMapEntry::Vacant(_) = set {
                                    slots_messages.entry(account.slot).or_default().push_back(
                                        SendMessage::Account((
                                            vec![FilterType::TokenkegOwner],
                                            account.clone(),
                                        )),
                                    );
                                }
                                set.or_default().insert(account.clone());
                            } else {
                                tokenkeg_owner_waiting_accounts.push(account.clone());
                            }

                            if let Some(delegate) = account.token_delegate().expect("bad filter") {
                                if filter.contains_tokenkeg_delegate(&delegate) {
                                    tokenkeg_delegate_touched_accounts.insert(account.pubkey);
                                    let set = tokenkeg_delegate_accounts.entry(account.pubkey);
                                    if let HashMapEntry::Vacant(_) = set {
                                        slots_messages.entry(account.slot).or_default().push_back(
                                            SendMessage::Account((
                                                vec![FilterType::TokenkegDelegate],
                                                account.clone(),
                                            )),
                                        );
                                    }
                                    set.or_default().insert(account);
                                } else {
                                    tokenkeg_delegate_waiting_accounts.push(account);
                                }
                            } else {
                                tokenkeg_delegate_waiting_accounts.push(account);
                            }
                        }
                    }
                    Some(Message::UpdateSlot((status, slot))) => {
                        if status == SlotStatus::Processed {
                            drain_tokenkeg_accounts(
                                &filter,
                                &mut tokenkeg_owner_accounts,
                                &mut tokenkeg_owner_waiting_accounts,
                                &mut tokenkeg_owner_touched_accounts,
                                &mut tokenkeg_delegate_accounts,
                                &mut tokenkeg_delegate_waiting_accounts,
                                &mut tokenkeg_delegate_touched_accounts,
                                &mut slots_messages,
                            );
                            compact_messages(&mut slots_messages, slot);
                        }
                        if Some(status) == commitment_level {
                            current_slot = slot;
                        }
                        if is_slot_messages_enabled {
                            messages.push(SendMessage::Slot((status, slot)));
                        }
                    }
                    Some(Message::StartupFinished) => unreachable!(),
                    Some(Message::Shutdown) | None => {
                        break 'outer;
                    }
                }
            }
            fetch_slots_messages(current_slot, &mut slots_messages, &mut messages);

            let _ = stats_queued_tx.send(());
            let client = client.clone();
            let queue_url = queue_url.clone();
            let stats_processed_tx = stats_processed_tx.clone();
            let send_jobs = Arc::clone(&send_jobs);
            send_jobs.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let ts = Instant::now();
                if let Err(error) = Self::send_messages(client, queue_url, messages).await {
                    error!("failed to send data: {:?}", error);
                }
                let _ = stats_processed_tx.send(ts.elapsed());
                send_jobs.fetch_sub(1, Ordering::Relaxed);
            });
        }
        send_jobs.fetch_sub(1, Ordering::Relaxed);

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
                    SendMessage::Account((filters, account)) => json!({
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
                                SendMessage::Account((_filters, account)) => {
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
