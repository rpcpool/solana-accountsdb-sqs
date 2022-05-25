use {
    super::{
        config::{AccountDataCompression, Config, ConfigAwsAuth, ConfigAwsSqs},
        filter::{AccountsFilter, TransactionsFilter},
        prom::{UploadTotalStatus, UPLOAD_QUEUE_SIZE, UPLOAD_REQUESTS, UPLOAD_TOTAL},
    },
    arrayref::array_ref,
    async_trait::async_trait,
    futures::future::FutureExt,
    log::*,
    rusoto_core::{HttpClient, RusotoError},
    rusoto_credential::{
        AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProfileProvider,
        ProvideAwsCredentials, StaticProvider,
    },
    rusoto_sqs::{
        GetQueueAttributesError, GetQueueAttributesRequest, SendMessageBatchError,
        SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs, SqsClient as RusotoSqsClient,
    },
    serde::{Deserialize, Serialize},
    serde_json::json,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
        SlotStatus as GeyserSlotStatus,
    },
    solana_sdk::{
        clock::UnixTimestamp,
        message::Message as TransactionMessage,
        program_pack::Pack,
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::Signature,
        transaction::Transaction,
    },
    solana_transaction_status::UiTransactionStatusMeta,
    spl_token::state::Account as SplTokenAccount,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, LinkedList},
        convert::TryInto,
        io::Result as IoResult,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    },
    thiserror::Error,
    tokio::{
        sync::{mpsc, Semaphore},
        time::sleep as sleep_async,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize, derivative::Derivative)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    Processed,
    Confirmed,
    #[derivative(Default)]
    Finalized,
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
            let pubkey_array = *array_ref!(&self.data, 32, PUBKEY_BYTES);
            Some(Pubkey::new_from_array(pubkey_array))
        } else {
            None
        }
    }

    pub fn token_delegate(&self) -> Option<Option<Pubkey>> {
        if self.owner == spl_token::ID && self.data.len() == SplTokenAccount::LEN {
            Some(match *array_ref!(&self.data, 72, 4) {
                [0, 0, 0, 0] => None,
                [1, 0, 0, 0] => {
                    let pubkey = Pubkey::new_from_array(*array_ref!(&self.data, 76, PUBKEY_BYTES));
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

#[derive(Debug, Clone)]
pub struct ReplicaTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: Transaction,
    pub meta: UiTransactionStatusMeta,
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
}

#[derive(Debug)]
pub struct ReplicaBlockMetadata {
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum Message {
    UpdateSlot((SlotStatus, u64)),
    UpdateAccount(ReplicaAccountInfo),
    NotifyTransaction(ReplicaTransactionInfo),
    NotifyBlockMetadata(ReplicaBlockMetadata),
    StartupFinished,
    Shutdown,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum SendMessage {
    Slot((SlotStatus, u64)),
    Account((ReplicaAccountInfo, Vec<String>)),
    Transaction(ReplicaTransactionInfo),
}

impl SendMessage {
    fn payload(&self, compression: &AccountDataCompression) -> IoResult<String> {
        Ok(match self {
            SendMessage::Slot((status, slot)) => json!({
                "type": "slot",
                "status": status,
                "slot": slot,
            }),
            SendMessage::Account((account, filters)) => json!({
                "type": "account",
                "filters": filters,
                "pubkey": account.pubkey.to_string(),
                "lamports": account.lamports,
                "owner": account.owner.to_string(),
                "executable": account.executable,
                "rent_epoch": account.rent_epoch,
                "data": base64::encode(compression.compress(&account.data)?.as_ref()),
                "write_version": account.write_version,
                "slot": account.slot,
            }),
            SendMessage::Transaction(transaction) => json!({
                "type": "transaction",
                "signature": transaction.signature.to_string(),
                "transaction": base64::encode(&bincode::serialize(&transaction.transaction).unwrap()),
                "meta": transaction.meta,
                "slot": transaction.slot,
                "block_time": transaction.block_time.unwrap_or_default(),
            }),
        }
        .to_string())
    }
}

#[derive(Debug)]
struct SendMessageWithPayload {
    message: SendMessage,
    payload: String,
}

#[derive(Debug, Error)]
pub enum SqsClientError {
    #[error("invalid commitment_level: {0:?}")]
    InvalidCommitmentLevel(SlotStatus),
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
    send_queue: mpsc::UnboundedSender<Message>,
    startup_job: Arc<AtomicBool>,
    send_job: Arc<AtomicBool>,
}

impl AwsSqsClient {
    pub async fn new(config: Config) -> SqsClientResult<Self> {
        if !matches!(
            config.messages.commitment_level,
            SlotStatus::Confirmed | SlotStatus::Finalized
        ) {
            return Err(SqsClientError::InvalidCommitmentLevel(
                config.messages.commitment_level,
            ));
        }

        Self::check_aws(&config).await?;

        let startup_job = Arc::new(AtomicBool::new(true));
        let startup_job_loop = Arc::clone(&startup_job);
        let send_job = Arc::new(AtomicBool::new(true));
        let send_job_loop = Arc::clone(&send_job);

        let (send_queue, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            if let Err(error) = Self::send_loop(config, rx, startup_job_loop, send_job_loop).await {
                error!("update_loop failed: {:?}", error);
            }
        });

        Ok(Self {
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

    // Check that SQS is available
    async fn check_aws(config: &Config) -> SqsClientResult<()> {
        let (client, queue_url) = Self::create_sqs(config.sqs.clone())?;
        client
            .get_queue_attributes(GetQueueAttributesRequest {
                attribute_names: None,
                queue_url: queue_url.clone(),
            })
            .await?;
        Ok(())
    }

    fn send_message(&self, message: Message) -> SqsClientResult {
        self.send_queue
            .send(message)
            .map_err(|_| SqsClientError::UpdateQueueChannelClosed)
    }

    pub async fn shutdown(self) {
        if self.send_message(Message::Shutdown).is_ok() {
            while self.send_job.load(Ordering::Relaxed) {
                sleep_async(Duration::from_micros(10)).await;
            }
        }
    }

    pub fn startup_finished(&self) -> SqsClientResult {
        self.send_message(Message::StartupFinished)?;
        while self.startup_job.load(Ordering::Relaxed) {
            sleep(Duration::from_micros(10));
        }
        Ok(())
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

    pub fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> SqsClientResult {
        let ReplicaTransactionInfoVersions::V0_0_1(transaction) = transaction;
        let message = transaction.transaction.message();
        self.send_message(Message::NotifyTransaction(ReplicaTransactionInfo {
            signature: *transaction.signature,
            is_vote: transaction.is_vote,
            transaction: Transaction {
                signatures: transaction.transaction.signatures().into(),
                message: TransactionMessage {
                    header: message.header().clone(),
                    account_keys: message.account_keys_iter().cloned().collect(),
                    recent_blockhash: *message.recent_blockhash(),
                    instructions: message.instructions().to_vec(),
                },
            },
            meta: transaction.transaction_status_meta.clone().into(),
            slot,
            block_time: None,
        }))
    }

    pub fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> SqsClientResult {
        let ReplicaBlockInfoVersions::V0_0_1(info) = blockinfo;
        self.send_message(Message::NotifyBlockMetadata(ReplicaBlockMetadata {
            slot: info.slot,
            block_time: info.block_time,
        }))
    }

    async fn send_loop(
        config: Config,
        mut rx: mpsc::UnboundedReceiver<Message>,
        startup_job: Arc<AtomicBool>,
        send_job: Arc<AtomicBool>,
    ) -> SqsClientResult {
        let sqs_max_requests = config.sqs.max_requests;
        let commitment_level = config.messages.commitment_level;
        let account_data_compression = config.messages.account_data_compression;
        let (client, queue_url) = Self::create_sqs(config.sqs)?;
        let is_slot_messages_enabled = config.slots.enabled;
        let accounts_filter = AccountsFilter::new(config.accounts_filters);
        log::info!("Sqs accounts filters: {:#?}", accounts_filter);
        let transactions_filter = TransactionsFilter::new(config.transactions_filter);
        log::info!("Sqs transactions filter: {:#?}", transactions_filter);

        // Save required Tokenkeg Accounts
        let mut tokenkeg_owner_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        let mut tokenkeg_delegate_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        while let Some(message) = rx.recv().await {
            match message {
                Message::UpdateSlot(_) => unreachable!(),
                Message::UpdateAccount(account) => {
                    if let Some(owner) = account.token_owner() {
                        if !accounts_filter.match_tokenkeg_owner(&owner).is_empty() {
                            tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                        }
                    }
                    if let Some(Some(delegate)) = account.token_delegate() {
                        if !accounts_filter
                            .match_tokenkeg_delegate(&delegate)
                            .is_empty()
                        {
                            tokenkeg_delegate_accounts.insert(account.pubkey, account.clone());
                        }
                    }
                }
                Message::NotifyTransaction(_) => unreachable!(),
                Message::NotifyBlockMetadata(_) => unreachable!(),
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

        // Messages, accounts and tokenkeg history changes
        let mut messages: LinkedList<SendMessageWithPayload> = LinkedList::new();
        let mut accounts: BTreeMap<u64, BTreeSet<ReplicaAccountInfo>> = BTreeMap::new();
        type TokenkegHist = BTreeMap<u64, HashMap<Pubkey, Option<ReplicaAccountInfo>>>;
        let mut tokenkeg_owner_accounts_hist: TokenkegHist = BTreeMap::new();
        let mut tokenkeg_delegate_accounts_hist: TokenkegHist = BTreeMap::new();
        let mut transactions: BTreeMap<u64, Vec<ReplicaTransactionInfo>> = BTreeMap::new();
        let mut blocks: BTreeMap<u64, ReplicaBlockMetadata> = BTreeMap::new();

        // Add message to the tail and increase counter
        fn add_message(
            messages: &mut LinkedList<SendMessageWithPayload>,
            message: SendMessage,
            account_data_compression: &AccountDataCompression,
        ) {
            match message.payload(account_data_compression) {
                Ok(payload) => {
                    messages.push_back(SendMessageWithPayload { message, payload });
                    UPLOAD_QUEUE_SIZE.inc();
                }
                Err(error) => {
                    error!("failed to create payload: {:?}", error);
                    UPLOAD_TOTAL
                        .with_label_values(&[UploadTotalStatus::Dropped.as_str()])
                        .inc();
                }
            }
        }

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
        #[allow(clippy::too_many_arguments)]
        fn generate_messages(
            messages: &mut LinkedList<SendMessageWithPayload>,
            accounts: &BTreeSet<ReplicaAccountInfo>,
            accounts_filter: &AccountsFilter,
            tokenkeg_owner_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_delegate_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_owner_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
            tokenkeg_delegate_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
            transactions: &[ReplicaTransactionInfo],
            transactions_filter: &TransactionsFilter,
            account_data_compression: &AccountDataCompression,
        ) {
            for account in accounts {
                let mut filters = accounts_filter.create_match();

                filters
                    .owner
                    .extend(accounts_filter.match_owner(&account.owner).iter());
                filters
                    .data_size
                    .extend(accounts_filter.match_data_size(account.data.len()).iter());

                if accounts_filter.match_tokenkeg(account) {
                    let owner = account.token_owner().expect("valid tokenkeg");
                    if let Some((pubkey, value, set)) =
                        match tokenkeg_owner_accounts.get(&account.pubkey) {
                            Some(existed)
                                if existed.token_owner().expect("valid tokenkeg") != owner =>
                            {
                                let set = accounts_filter.match_tokenkeg_owner(&owner);
                                let prev = if set.is_empty() {
                                    tokenkeg_owner_accounts.remove(&account.pubkey)
                                } else {
                                    tokenkeg_owner_accounts.insert(account.pubkey, account.clone())
                                };
                                Some((account.pubkey, prev, set))
                            }
                            None => {
                                let set = accounts_filter.match_tokenkeg_owner(&owner);
                                if !set.is_empty() {
                                    tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                                    Some((account.pubkey, None, set))
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    {
                        tokenkeg_owner_accounts_hist.entry(pubkey).or_insert(value);
                        filters.tokenkeg_owner.extend(set.iter());
                    }

                    let delegate = account.token_delegate().expect("valid tokenkeg");
                    if let Some((pubkey, value, set)) = match (
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
                            let set = accounts_filter.match_tokenkeg_delegate(&delegate);
                            let prev = if set.is_empty() {
                                tokenkeg_delegate_accounts.remove(&account.pubkey)
                            } else {
                                tokenkeg_delegate_accounts.insert(account.pubkey, account.clone())
                            };
                            Some((account.pubkey, prev, set))
                        }
                        (Some(delegate), None) => {
                            let set = accounts_filter.match_tokenkeg_delegate(&delegate);
                            if !set.is_empty() {
                                tokenkeg_delegate_accounts.insert(account.pubkey, account.clone());
                                Some((account.pubkey, None, set))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    } {
                        tokenkeg_delegate_accounts_hist
                            .entry(pubkey)
                            .or_insert(value);
                        filters.tokenkeg_delegate.extend(set.iter());
                    }
                }

                let filters = filters.get_filters();
                if !filters.is_empty() {
                    add_message(
                        messages,
                        SendMessage::Account((account.clone(), filters)),
                        account_data_compression,
                    );
                }
            }

            for transaction in transactions {
                if transactions_filter.contains(transaction) {
                    add_message(
                        messages,
                        SendMessage::Transaction(transaction.clone()),
                        account_data_compression,
                    );
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
        let send_jobs = Arc::new(Semaphore::new(sqs_max_requests));
        let mut status_current_slot = 0;
        let mut account_current_slot = 0;
        let mut transaction_current_slot = 0;
        loop {
            if !messages.is_empty() && send_jobs.available_permits() > 0 {
                // The maximum allowed individual message size and the maximum total payload size (the sum of the
                // individual lengths of all of the batched messages) are both 256 KB (262,144 bytes).
                let mut messages_batch_size = 0;
                let mut messages_batch = Vec::with_capacity(10);
                while messages_batch.len() < 10 {
                    match messages.pop_front() {
                        Some(message) if messages_batch_size + message.payload.len() < 262_000 => {
                            messages_batch_size += message.payload.len();
                            messages_batch.push(message)
                        }
                        Some(message) => {
                            // TODO: upload to S3
                            // Drop if message payload more than allowed size
                            if messages_batch.is_empty() {
                                let kind = match message.message {
                                    SendMessage::Account((account, _filters)) => {
                                        format!("account {}", account.pubkey)
                                    }
                                    message => format!("{:?}", message),
                                };
                                error!("drop message because payload is too large: {}", kind);
                                UPLOAD_QUEUE_SIZE.dec();
                                UPLOAD_TOTAL
                                    .with_label_values(&[UploadTotalStatus::Dropped.as_str()])
                                    .inc();
                            } else {
                                messages.push_front(message);
                            }
                            break;
                        }
                        None => break,
                    };
                }
                let messages_count = messages_batch.len();
                if messages_count != 0 {
                    UPLOAD_QUEUE_SIZE.sub(messages_count as i64);
                    let client = client.clone();
                    let queue_url = queue_url.clone();
                    let send_jobs = Arc::clone(&send_jobs);
                    let send_permit = send_jobs.try_acquire_owned().expect("available permit");
                    tokio::spawn(async move {
                        UPLOAD_REQUESTS.inc();
                        let status =
                            match Self::send_messages(client, queue_url, messages_batch).await {
                                Ok(()) => UploadTotalStatus::Success,
                                Err(error) => {
                                    error!("failed to send data: {:?}", error);
                                    UploadTotalStatus::Failed
                                }
                            };
                        UPLOAD_TOTAL.with_label_values(&[status.as_str()]).inc();
                        UPLOAD_REQUESTS.dec();

                        drop(send_permit);
                    });
                }
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
                    Some(Message::UpdateSlot((status, slot))) => {
                        if is_slot_messages_enabled {
                            add_message(&mut messages, SendMessage::Slot((status, slot)), &account_data_compression);
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
                                remove_outdated_slots(&mut transactions, min_slot);
                                remove_outdated_slots(&mut blocks, min_slot);
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

                            match (accounts.get(&slot), transactions.get_mut(&slot), blocks.get(&slot)) {
                                (Some(accounts), Some(transactions), Some(block)) => {
                                    for transaction in transactions.iter_mut() {
                                        transaction.block_time = block.block_time;
                                    }

                                    generate_messages(
                                        &mut messages,
                                        accounts,
                                        &accounts_filter,
                                        &mut tokenkeg_owner_accounts,
                                        &mut tokenkeg_delegate_accounts,
                                        tokenkeg_owner_accounts_hist.entry(slot).or_default(),
                                        tokenkeg_delegate_accounts_hist.entry(slot).or_default(),
                                        transactions,
                                        &transactions_filter,
                                        &account_data_compression
                                    );
                                }
                                _ => error!(
                                    "send_loop error: accounts/transactions/block for slot {} does not exists",
                                    slot
                                ),
                            }

                            status_current_slot = slot;
                        }
                    }
                    Some(Message::UpdateAccount(account)) => {
                        if account_current_slot != account.slot {
                            // Drop previous accounts changes
                            accounts.insert(account.slot, Default::default());
                            account_current_slot = account.slot;
                        }

                        accounts.get_mut(&account.slot).unwrap().insert(account);
                    }
                    Some(Message::NotifyTransaction(transaction)) => {
                        if transaction_current_slot != transaction.slot {
                            // Drop previous transactions
                            transactions.insert(transaction.slot, Default::default());
                            transaction_current_slot = transaction.slot;
                        }

                        transactions.get_mut(&transaction.slot).unwrap().push(transaction);
                    }
                    Some(Message::NotifyBlockMetadata(block)) => {
                        blocks.insert(block.slot, block);
                    }
                    Some(Message::StartupFinished) => unreachable!(),
                    Some(Message::Shutdown) | None => break,
                }
            }
        }
        let _ = send_jobs
            .acquire_many(sqs_max_requests.try_into().expect("valid size"))
            .await
            .expect("alive");
        send_job.store(false, Ordering::Relaxed);
        info!("update_loop finished");

        Ok(())
    }

    async fn send_messages(
        client: RusotoSqsClient,
        queue_url: String,
        messages: Vec<SendMessageWithPayload>,
    ) -> SqsClientResult {
        let (messages, entries): (Vec<SendMessage>, Vec<_>) = messages
            .into_iter()
            .enumerate()
            .map(|(id, message)| {
                (
                    message.message,
                    SendMessageBatchRequestEntry {
                        id: id.to_string(),
                        message_body: message.payload,
                        ..Default::default()
                    },
                )
            })
            .unzip();

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
                                SendMessage::Slot((status, slot)) => {
                                    value["status"] = json!(status);
                                    value["slot"] = json!(slot);
                                }
                                SendMessage::Account((account, _filters)) => {
                                    value["pubkey"] = json!(account.pubkey.to_string());
                                    value["slot"] = json!(account.slot);
                                    value["write_version"] = json!(account.write_version);
                                }
                                SendMessage::Transaction(transaction) => {
                                    value["signature"] = json!(transaction.signature.to_string());
                                    value["slot"] = json!(transaction.slot);
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

#[allow(clippy::large_enum_variant)]
enum AwsCredentialsProvider {
    Static(StaticProvider),
    Chain(AutoRefreshingProvider<ChainProvider>),
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
            ConfigAwsAuth::Chain {
                credentials_file,
                profile,
            } => {
                let profile_provider = match (credentials_file, profile) {
                    (Some(file_path), Some(profile)) => {
                        ProfileProvider::with_configuration(file_path, profile)
                    }
                    (Some(file_path), None) => {
                        ProfileProvider::with_default_configuration(file_path)
                    }
                    (None, Some(profile)) => ProfileProvider::with_default_credentials(profile)?,
                    (None, None) => ProfileProvider::new()?,
                };
                Ok(Self::Chain(AutoRefreshingProvider::new(
                    ChainProvider::with_profile_provider(profile_provider),
                )?))
            }
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for AwsCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        match self {
            Self::Static(p) => p.credentials(),
            Self::Chain(p) => p.credentials(),
        }
        .await
    }
}
