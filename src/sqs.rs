use {
    crate::{
        aws::{AwsError, S3Client, SqsClient},
        config::{AccountsDataCompression, Config},
        filters::{Filters, FiltersError},
        metrics::{
            health::{set_health, HealthInfoType},
            UploadMessagesStatus, SLOTS_LAST_PROCESSED, UPLOAD_MESSAGES_TOTAL, UPLOAD_MISSIED_INFO,
            UPLOAD_QUEUE_SIZE,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoVersions, ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
        SlotStatus as GeyserSlotStatus,
    },
    arrayref::array_ref,
    base64::{engine::general_purpose, Engine as _},
    futures::{
        future::FutureExt,
        stream::{self, StreamExt},
    },
    log::*,
    md5::{Digest, Md5},
    rusoto_sqs::SendMessageBatchRequestEntry,
    serde::{Deserialize, Serialize},
    serde_json::{json, Value},
    solana_sdk::{
        clock::UnixTimestamp,
        program_pack::Pack,
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::Signature,
        transaction::SanitizedTransaction,
    },
    solana_transaction_status::UiTransactionStatusMeta,
    spl_token::state::Account as SplTokenAccount,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap, LinkedList},
        convert::{TryFrom, TryInto},
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SlotStatus {
    Processed,
    Confirmed,
    #[default]
    Finalized,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead,
}

impl SlotStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::FirstShredReceived => "firstShredReceived",
            Self::Completed => "completed",
            Self::CreatedBank => "createdBank",
            Self::Dead => "Dead",
        }
    }
}

impl From<&GeyserSlotStatus> for SlotStatus {
    fn from(status: &GeyserSlotStatus) -> Self {
        match status {
            GeyserSlotStatus::Processed => Self::Processed,
            GeyserSlotStatus::Confirmed => Self::Confirmed,
            GeyserSlotStatus::Rooted => Self::Finalized,
            GeyserSlotStatus::FirstShredReceived => Self::FirstShredReceived,
            GeyserSlotStatus::Completed => Self::Completed,
            GeyserSlotStatus::CreatedBank => Self::CreatedBank,
            GeyserSlotStatus::Dead(_s) => Self::Dead,
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
    pub txn_signature: Option<Signature>,
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
            ReplicaAccountInfoVersions::V0_0_1(_account) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_2(_account) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_3(account) => Self {
                pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
                lamports: account.lamports,
                owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: account.data.into(),
                write_version: account.write_version,
                slot,
                txn_signature: account.txn.map(|tx| *tx.signature()),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaTransactionInfo {
    pub signature: Signature,
    pub is_vote: bool,
    pub transaction: SanitizedTransaction,
    pub meta: UiTransactionStatusMeta,
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
    pub index: usize,
}

impl<'a> From<(ReplicaTransactionInfoVersions<'a>, u64)> for ReplicaTransactionInfo {
    fn from((transaction, slot): (ReplicaTransactionInfoVersions<'a>, u64)) -> Self {
        match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_transaction) => {
                unreachable!("ReplicaTransactionInfoVersions::V0_0_1 is not supported")
            }
            ReplicaTransactionInfoVersions::V0_0_2(transaction) => Self {
                signature: *transaction.signature,
                is_vote: transaction.is_vote,
                transaction: transaction.transaction.clone(),
                meta: transaction.transaction_status_meta.clone().into(),
                slot,
                block_time: None,
                index: transaction.index,
            },
        }
    }
}

#[derive(Debug)]
pub struct ReplicaBlockMetadata {
    pub slot: u64,
    pub block_time: Option<UnixTimestamp>,
}

impl From<ReplicaBlockInfoVersions<'_>> for ReplicaBlockMetadata {
    fn from(blockinfo: ReplicaBlockInfoVersions) -> Self {
        match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_3(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_3 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_4(info) => Self {
                slot: info.slot,
                block_time: info.block_time,
            },
        }
    }
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

#[derive(Debug, PartialEq, Eq)]
pub enum SendMessageType {
    Slot,
    Account,
    Transaction,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum SendMessage {
    Slot((SlotStatus, u64)),
    Account((ReplicaAccountInfo, Vec<String>)),
    Transaction((ReplicaTransactionInfo, Vec<String>)),
}

impl SendMessage {
    const fn get_type(&self) -> SendMessageType {
        match self {
            Self::Slot(_) => SendMessageType::Slot,
            Self::Account(_) => SendMessageType::Account,
            Self::Transaction(_) => SendMessageType::Transaction,
        }
    }

    fn payload(&self, compression: AccountsDataCompression) -> IoResult<(String, String)> {
        let mut md5 = Md5::new();
        let value = match self {
            SendMessage::Slot((status, slot)) => {
                md5.update("slot");
                md5.update(slot.to_le_bytes());
                md5.update(status.as_str());
                json!({
                    "type": "slot",
                    "status": status,
                    "slot": slot,
                })
            }
            SendMessage::Account((account, filters)) => {
                md5.update("account");
                md5.update(account.pubkey.as_ref());
                md5.update(account.lamports.to_le_bytes());
                md5.update(&account.data);
                md5.update(account.slot.to_le_bytes());
                json!({
                    "type": "account",
                    "filters": filters,
                    "pubkey": account.pubkey.to_string(),
                    "lamports": account.lamports,
                    "owner": account.owner.to_string(),
                    "executable": account.executable,
                    "rent_epoch": account.rent_epoch,
                    "data": general_purpose::STANDARD.encode(compression.compress(&account.data)?.as_ref()),
                    "write_version": account.write_version,
                    "txn_signature": account.txn_signature.map(|s| s.to_string()).unwrap_or_default(),
                    "slot": account.slot,
                })
            }
            SendMessage::Transaction((transaction, filters)) => {
                md5.update("transaction");
                md5.update(transaction.slot.to_le_bytes());
                md5.update(transaction.signature.as_ref());
                json!({
                    "type": "transaction",
                    "filters": filters,
                    "signature": transaction.signature.to_string(),
                    "transaction": general_purpose::STANDARD.encode(bincode::serialize(&transaction.transaction.to_versioned_transaction()).unwrap()),
                    "meta": transaction.meta,
                    "slot": transaction.slot,
                    "block_time": transaction.block_time.unwrap_or_default(),
                    "index": transaction.index,
                })
            }
        };
        Ok((value.to_string(), hex::encode(md5.finalize())))
    }

    fn payload_short(&self, s3: &str) -> String {
        match self {
            SendMessage::Slot(_) => json!({
                "type": "slot",
                "s3": s3,
            }),
            SendMessage::Account((_account, filters)) => json!({
                "type": "account",
                "filters": filters,
                "s3": s3,
            }),
            SendMessage::Transaction((_transaction, filters)) => json!({
                "type": "transaction",
                "filters": filters,
                "s3": s3,
            }),
        }
        .to_string()
    }

    fn s3_key(&self, node: &str) -> String {
        match self {
            Self::Slot((status, slot)) => {
                warn!("Slot is not expected to be uploaded to S3");
                format!("{}/slot/{}-{:?}", node, slot, status)
            }
            Self::Account((account, _filters)) => {
                format!(
                    "{}/account/{}/{}-{}",
                    node, account.slot, account.pubkey, account.write_version
                )
            }
            Self::Transaction((transaction, _filters)) => {
                warn!("Transaction is not expected to be uploaded to S3");
                format!(
                    "{}/transaction/{}/{}",
                    node, transaction.slot, transaction.signature
                )
            }
        }
    }

    fn update_info(&self, mut value: Value) -> Value {
        match self {
            SendMessage::Slot((status, slot)) => {
                value["status"] = json!(status);
                value["slot"] = json!(slot);
            }
            SendMessage::Account((account, _filters)) => {
                value["pubkey"] = json!(account.pubkey.to_string());
                value["slot"] = json!(account.slot);
                value["write_version"] = json!(account.write_version);
            }
            SendMessage::Transaction((transaction, _filters)) => {
                value["signature"] = json!(transaction.signature.to_string());
                value["slot"] = json!(transaction.slot);
            }
        };
        value
    }
}

#[derive(Debug)]
struct SendMessageWithPayload {
    message: SendMessage,
    compression: &'static str,
    s3: bool,
    md5: String,
    payload: String,
}

impl SendMessageWithPayload {
    const S3_SIZE: usize = 250;

    fn new(
        message: SendMessage,
        accounts_data_compression: AccountsDataCompression,
    ) -> IoResult<Self> {
        message
            .payload(accounts_data_compression)
            .map(|(payload, md5)| Self {
                message,
                compression: accounts_data_compression.as_str(),
                s3: payload.len() > SqsClient::REQUEST_LIMIT,
                md5,
                payload,
            })
    }

    fn payload_size(&self) -> usize {
        if self.s3 {
            Self::S3_SIZE
        } else {
            self.payload.len()
        }
    }
}

#[derive(Debug, Error)]
pub enum SqsClientError {
    #[error("invalid commitment_level: {0:?}")]
    InvalidCommitmentLevel(SlotStatus),
    #[error("aws error: {0}")]
    Aws(#[from] AwsError),
    #[error("filters error: {0}")]
    Filters(#[from] FiltersError),
    #[error("send message through send queue failed: channel is closed")]
    UpdateQueueChannelClosed,
}

pub type SqsClientResult<T = ()> = Result<T, SqsClientError>;

#[derive(Debug)]
pub struct AwsSqsClient {
    send_queue: mpsc::UnboundedSender<Message>,
    send_queue_error: AtomicBool,
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

        SqsClient::new(config.sqs.clone(), "")?.check().await?;
        S3Client::new(config.s3.clone())?.check().await?;
        let filters = Arc::new(
            Filters::new(
                config.filters.clone(),
                config.redis.clone(),
                config.node.clone(),
                config.log.filters,
            )
            .await?,
        );

        let startup_job = Arc::new(AtomicBool::new(true));
        let startup_job_loop = Arc::clone(&startup_job);
        let send_job = Arc::new(AtomicBool::new(true));
        let send_job_loop = Arc::clone(&send_job);

        let (send_queue, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            set_health(HealthInfoType::SendLoop, Ok(()));
            if let Err(error) =
                Self::send_loop(config, filters, rx, startup_job_loop, send_job_loop).await
            {
                error!("update_loop failed: {:?}", error);
            }
            set_health(HealthInfoType::SendLoop, Err(()));
        });

        Ok(Self {
            send_queue,
            send_queue_error: AtomicBool::new(false),
            startup_job,
            send_job,
        })
    }

    fn send_message(&self, message: Message) -> SqsClientResult {
        if self.send_queue.send(message).is_err() && !self.send_queue_error.load(Ordering::SeqCst) {
            self.send_queue_error.store(true, Ordering::SeqCst);
            Err(SqsClientError::UpdateQueueChannelClosed)
        } else {
            Ok(())
        }
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

    pub fn update_slot(&self, slot: u64, status: SlotStatus) -> SqsClientResult {
        self.send_message(Message::UpdateSlot((status, slot)))
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
        self.send_message(Message::NotifyTransaction((transaction, slot).into()))
    }

    pub fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> SqsClientResult {
        self.send_message(Message::NotifyBlockMetadata(blockinfo.into()))
    }

    async fn send_loop(
        config: Config,
        filters: Arc<Filters>,
        mut rx: mpsc::UnboundedReceiver<Message>,
        startup_job: Arc<AtomicBool>,
        send_job: Arc<AtomicBool>,
    ) -> SqsClientResult {
        let sqs_max_requests = config.sqs.max_requests;
        let commitment_level = config.messages.commitment_level;
        let accounts_data_compression = config.messages.accounts_data_compression;
        let sqs = SqsClient::new(config.sqs, &config.node)?;
        let s3 = S3Client::new(config.s3)?;

        // Save required Tokenkeg Accounts
        let mut tokenkeg_owner_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        let mut tokenkeg_delegate_accounts: HashMap<Pubkey, ReplicaAccountInfo> = HashMap::new();
        let accounts_filter = filters.create_accounts_match().await;
        while let Some(message) = rx.recv().await {
            match message {
                Message::UpdateSlot(_) => unreachable!(),
                Message::UpdateAccount(account) => {
                    if let Some(owner) = account.token_owner() {
                        if accounts_filter.contains_tokenkeg_owner(&owner) {
                            tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                        }
                    }
                    if let Some(Some(delegate)) = account.token_delegate() {
                        if accounts_filter.contains_tokenkeg_delegate(&delegate) {
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
                    drop(accounts_filter);
                    Filters::shutdown(filters).await;
                    send_job.store(false, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
        drop(accounts_filter);
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
            accounts_data_compression: AccountsDataCompression,
        ) {
            match SendMessageWithPayload::new(message, accounts_data_compression) {
                Ok(message) => {
                    messages.push_back(message);
                    UPLOAD_QUEUE_SIZE.inc();
                }
                Err(error) => {
                    error!("failed to create payload: {:?}", error);
                    UPLOAD_MESSAGES_TOTAL
                        .with_label_values(&[UploadMessagesStatus::Dropped.as_str()])
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
        async fn generate_messages(
            filters: &Filters,
            messages: &mut LinkedList<SendMessageWithPayload>,
            accounts: &BTreeSet<ReplicaAccountInfo>,
            tokenkeg_owner_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_delegate_accounts: &mut HashMap<Pubkey, ReplicaAccountInfo>,
            tokenkeg_owner_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
            tokenkeg_delegate_accounts_hist: &mut HashMap<Pubkey, Option<ReplicaAccountInfo>>,
            transactions: &[ReplicaTransactionInfo],
            accounts_data_compression: AccountsDataCompression,
        ) {
            let mut account_filters = filters.create_accounts_match().await;
            for account in accounts {
                account_filters.reset();

                account_filters.match_account(&account.pubkey);
                account_filters.match_owner(&account.owner);
                account_filters.match_data_size(account.data.len());
                account_filters.match_serum_event_queue(&account.pubkey, &account.data);

                if account_filters.match_tokenkeg(account) {
                    let owner = account.token_owner().expect("valid tokenkeg");
                    match tokenkeg_owner_accounts.get(&account.pubkey) {
                        Some(existed)
                            if existed.token_owner().expect("valid tokenkeg") != owner =>
                        {
                            let prev = if account_filters.match_tokenkeg_owner(&owner) {
                                tokenkeg_owner_accounts.insert(account.pubkey, account.clone())
                            } else {
                                tokenkeg_owner_accounts.remove(&account.pubkey)
                            };
                            tokenkeg_owner_accounts_hist
                                .entry(account.pubkey)
                                .or_insert(prev);
                        }
                        None if account_filters.match_tokenkeg_owner(&owner) => {
                            tokenkeg_owner_accounts.insert(account.pubkey, account.clone());
                            tokenkeg_owner_accounts_hist
                                .entry(account.pubkey)
                                .or_insert(None);
                        }
                        _ => {}
                    }

                    let delegate = account.token_delegate().expect("valid tokenkeg");
                    match (
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
                            let prev = if account_filters.match_tokenkeg_delegate(&delegate) {
                                tokenkeg_delegate_accounts.insert(account.pubkey, account.clone())
                            } else {
                                tokenkeg_delegate_accounts.remove(&account.pubkey)
                            };
                            tokenkeg_delegate_accounts_hist
                                .entry(account.pubkey)
                                .or_insert(prev);
                        }
                        (Some(delegate), None) => {
                            if account_filters.match_tokenkeg_delegate(&delegate) {
                                tokenkeg_delegate_accounts.insert(account.pubkey, account.clone());
                                tokenkeg_delegate_accounts_hist
                                    .entry(account.pubkey)
                                    .or_insert(None);
                            }
                        }
                        _ => {}
                    }
                }

                let filters = account_filters.get_filters();
                if !filters.is_empty() {
                    add_message(
                        messages,
                        SendMessage::Account((account.clone(), filters)),
                        accounts_data_compression,
                    );
                }
            }
            drop(account_filters); // contains locked Mutex

            for transaction in transactions {
                let filters = filters.get_transaction_filters(transaction).await;
                if !filters.is_empty() {
                    add_message(
                        messages,
                        SendMessage::Transaction((transaction.clone(), filters)),
                        accounts_data_compression,
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
        let mut waited_slot = None;
        let mut status_current_slot = 0;
        let mut account_current_slot = 0;
        let mut transaction_current_slot = 0;

        macro_rules! handle_messages {
            ($slot:ident) => {
                waited_slot = None;

                // Remove outdated data (keep 320 slots)
                if let Some(min_slot) = $slot.checked_sub(320) {
                    remove_outdated_slots(&mut accounts, min_slot);
                    remove_outdated_slots(&mut tokenkeg_owner_accounts_hist, min_slot);
                    remove_outdated_slots(&mut tokenkeg_delegate_accounts_hist, min_slot);
                    remove_outdated_slots(&mut transactions, min_slot);
                    remove_outdated_slots(&mut blocks, min_slot);
                }

                // Handle reorg
                for slot in ($slot..=status_current_slot).rev() {
                    if let Some(hist) = tokenkeg_owner_accounts_hist.remove(&slot) {
                        tokenkeg_revert(&mut tokenkeg_owner_accounts, hist);
                    }
                    if let Some(hist) = tokenkeg_delegate_accounts_hist.remove(&slot) {
                        tokenkeg_revert(&mut tokenkeg_delegate_accounts, hist);
                    }
                }

                match (
                    accounts.get(&$slot),
                    transactions.get_mut(&$slot),
                    blocks.get(&$slot),
                ) {
                    (Some(accounts), transactions, Some(block)) => {
                        let mut txvec = vec![];
                        let transactions = transactions.unwrap_or_else(|| &mut txvec);
                        for transaction in transactions.iter_mut() {
                            transaction.block_time = block.block_time;
                        }

                        generate_messages(
                            &filters,
                            &mut messages,
                            accounts,
                            &mut tokenkeg_owner_accounts,
                            &mut tokenkeg_delegate_accounts,
                            tokenkeg_owner_accounts_hist.entry($slot).or_default(),
                            tokenkeg_delegate_accounts_hist.entry($slot).or_default(),
                            transactions,
                            accounts_data_compression,
                        ).await;
                    }
                    (accounts, transactions, block) => {
                        error!(
                            "accounts/transactions/block for slot {} does not exists ({} / {} / {})",
                            $slot,
                            accounts.map(|_| "y").unwrap_or_else(|| "n"),
                            transactions.map(|_| "y").unwrap_or_else(|| "n"),
                            block.map(|_| "y").unwrap_or_else(|| "n")
                        );
                        UPLOAD_MISSIED_INFO.inc();
                    }
                }

                status_current_slot = $slot;
            }
        }

        loop {
            if !messages.is_empty() && send_jobs.available_permits() > 0 {
                let mut messages_type = None;
                let mut messages_batch_size = 0;
                let mut messages_batch = Vec::with_capacity(10);
                while messages_batch.len() < 10 {
                    if let Some(message) = messages.pop_front() {
                        if messages_batch_size + message.payload_size() <= SqsClient::REQUEST_LIMIT
                            && (!sqs.is_typed()
                                || messages_type.is_none()
                                || messages_type == Some(message.message.get_type()))
                        {
                            if sqs.is_typed() {
                                messages_type = Some(message.message.get_type());
                            }
                            messages_batch_size += message.payload_size();
                            messages_batch.push(message);
                        } else {
                            messages.push_front(message);
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if !messages_batch.is_empty() {
                    UPLOAD_QUEUE_SIZE.sub(messages_batch.len() as i64);
                    let filters = Arc::clone(&filters);
                    let sqs = sqs.clone();
                    let s3 = s3.clone();
                    let send_jobs = Arc::clone(&send_jobs);
                    let send_permit = send_jobs.try_acquire_owned().expect("available permit");
                    tokio::spawn(async move {
                        Self::send_messages(filters, sqs, s3, messages_batch, messages_type).await;
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
                        SLOTS_LAST_PROCESSED
                            .with_label_values(&[status.as_str()])
                            .set(slot as i64);

                        if filters.is_slot_messages_enabled().await {
                            add_message(&mut messages, SendMessage::Slot((status, slot)), accounts_data_compression);
                        }

                        if status == commitment_level {
                            assert!(matches!(
                                commitment_level,
                                SlotStatus::Confirmed | SlotStatus::Finalized
                            ));

                            if let Some(waited) = waited_slot {
                                error!("new slot {}, without handling previous {}", slot, waited);
                                UPLOAD_MISSIED_INFO.inc();
                            }

                            if blocks.contains_key(&slot) {
                                handle_messages!(slot);
                            } else {
                                waited_slot = Some(slot);
                            }
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
                        let block_slot = block.slot;
                        blocks.insert(block.slot, block);
                        if waited_slot == Some(block_slot) {
                            handle_messages!(block_slot);
                        }
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
        Filters::shutdown(filters).await;
        send_job.store(false, Ordering::Relaxed);
        info!("update_loop finished");

        Ok(())
    }

    async fn send_messages(
        filters: Arc<Filters>,
        sqs: SqsClient,
        s3: S3Client,
        messages: Vec<SendMessageWithPayload>,
        messages_type: Option<SendMessageType>,
    ) {
        let mut success_count = 0;
        let mut failed_count = 0;

        let messages_initial_count = messages.len();
        let (mut messages, entries): (Vec<Option<SendMessage>>, Vec<_>) =
            stream::iter(messages.into_iter().enumerate())
                .filter_map(|(id, message)| {
                    let s3 = if message.s3 { Some(s3.clone()) } else { None };
                    let mut attributes = sqs.get_attributes();
                    async move {
                        let message_body = match s3 {
                            Some(s3) => {
                                let key = message.message.s3_key(
                                    attributes
                                        .get("node")
                                        .expect("node attribute should exists"),
                                );
                                if let Err(error) = s3
                                    .put_object(key.clone(), message.payload.into_bytes())
                                    .await
                                {
                                    let value = message.message.update_info(json!({}));
                                    error!(
                                        "failed to upload payload to s3 ({:?}): {:?}",
                                        error,
                                        serde_json::to_string(&value)
                                    );
                                    return None;
                                }
                                message.message.payload_short(&key)
                            }
                            None => message.payload,
                        };
                        attributes.insert("md5", message.md5);
                        attributes.insert("compression", message.compression);
                        Some((
                            Some(message.message),
                            SendMessageBatchRequestEntry {
                                id: id.to_string(),
                                message_body,
                                message_attributes: Some(attributes.into_inner()),
                                ..Default::default()
                            },
                        ))
                    }
                })
                .unzip()
                .await;
        failed_count += messages_initial_count - messages.len();

        let failed = sqs.send_batch(entries, messages_type).await;
        success_count += messages.len() - failed.len();
        failed_count += failed.len();
        for entry in failed {
            let index = entry.id.parse::<usize>().ok();
            if let Some(message) = index.and_then(|index| messages.get_mut(index)) {
                if let Some(message) = message {
                    let value = message.update_info(json!({
                        "code": entry.code,
                        "message": entry.message,
                        "sender_fault": entry.sender_fault,
                    }));
                    error!(
                        "failed to send sqs message: {:?}",
                        serde_json::to_string(&value)
                    );
                }
                *message = None;
            }
        }
        let entries = messages
            .into_iter()
            .filter_map(|message| match message {
                Some(SendMessage::Transaction((tx, names))) => Some((tx, names)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if !entries.is_empty() {
            filters.log_transaction(entries).await;
        }

        for (status, count) in [
            (UploadMessagesStatus::Success, success_count),
            (UploadMessagesStatus::Failed, failed_count),
        ] {
            if count > 0 {
                UPLOAD_MESSAGES_TOTAL
                    .with_label_values(&[status.as_str()])
                    .inc_by(count as u64);
            }
        }
    }
}
