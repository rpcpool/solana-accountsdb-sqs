use {
    super::config::{ConfigAwsAuth, ConfigAwsSqs},
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
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        ReplicaAccountInfoVersions, SlotStatus as AccountsDbSlotStatus,
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::{BTreeMap, LinkedList},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::{runtime::Runtime, sync::mpsc, time::sleep as sleep_async},
};

#[derive(Debug)]
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
    pub fn serialize(&self) -> String {
        serde_json::json!({
            "pubkey": self.pubkey.to_string(),
            "lamports": self.lamports,
            "owner": self.owner.to_string(),
            "executable": self.executable,
            "rent_epoch": self.rent_epoch,
            "data": base64::encode(&self.data),
            "write_version": self.write_version,
            "slot": self.slot,
        })
        .to_string()
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

#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize)]
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
    Shutdown,
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
    send_queue: mpsc::UnboundedSender<Message>,
    send_jobs: Arc<AtomicU64>,
}

impl AwsSqsClient {
    pub fn new(config: ConfigAwsSqs) -> SqsClientResult<Self> {
        let send_jobs = Arc::new(AtomicU64::new(1));
        let send_jobs_loop = Arc::clone(&send_jobs);
        let runtime = Runtime::new().map_err(SqsClientError::RuntimeCreate)?;
        let send_queue = runtime.block_on(async move {
            let max_requests = config.max_requests;
            let commitment_level = config.commitment_level;
            let (client, queue_url) = Self::create_sqs(config)?;
            client
                .get_queue_attributes(GetQueueAttributesRequest {
                    attribute_names: None,
                    queue_url: queue_url.clone(),
                })
                .await?;

            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                if let Err(error) = Self::send_loop(
                    client,
                    queue_url,
                    max_requests,
                    commitment_level,
                    rx,
                    send_jobs_loop,
                )
                .await
                {
                    error!("update_loop failed: {:?}", error);
                }
            });

            Ok::<_, SqsClientError>(tx)
        })?;
        Ok(Self {
            runtime,
            send_queue,
            send_jobs,
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

    pub fn update_account(&self, account: ReplicaAccountInfo) -> SqsClientResult {
        self.send_message(Message::UpdateAccount(account))
    }

    pub fn update_slot(&self, status: SlotStatus, slot: u64) -> SqsClientResult {
        self.send_message(Message::UpdateSlot((status, slot)))
    }

    async fn send_loop(
        client: RusotoSqsClient,
        queue_url: String,
        max_requests: u64,
        commitment_level: SlotStatus,
        mut rx: mpsc::UnboundedReceiver<Message>,
        send_jobs: Arc<AtomicU64>,
    ) -> SqsClientResult {
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

        let mut current_slot = 0;
        let mut accounts = BTreeMap::new();
        fn fetch_accounts(
            current_slot: u64,
            accounts: &mut BTreeMap<u64, LinkedList<ReplicaAccountInfo>>,
            messages: &mut Vec<ReplicaAccountInfo>,
        ) {
            while messages.len() < messages.capacity() {
                let slot = match accounts.keys().next().copied() {
                    Some(slot) if slot <= current_slot => slot,
                    _ => break,
                };
                let mut list = accounts.remove(&slot).unwrap();
                while messages.len() < messages.capacity() {
                    match list.pop_front() {
                        Some(message) => messages.push(message),
                        None => break,
                    };
                }
                if !list.is_empty() {
                    accounts.insert(slot, list);
                }
            }
        }

        let mut is_alive = true;
        while is_alive || !accounts.is_empty() {
            while send_jobs.load(Ordering::Relaxed) >= max_requests {
                // `tokio::time::sleep(Duration::from_nanos(100)).await` or some signal?
                tokio::task::yield_now().await;
            }

            let mut messages = Vec::with_capacity(10);
            fetch_accounts(current_slot, &mut accounts, &mut messages);
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
                        if current_slot >= account.slot {
                            messages.push(account);
                        } else {
                            accounts.entry(account.slot).or_default().push_back(account);
                        }
                    }
                    Some(Message::UpdateSlot((status, slot))) => {
                        if status == commitment_level {
                            current_slot = slot;
                        }
                    }
                    Some(Message::Shutdown) | None => {
                        is_alive = false;
                        break;
                    }
                }
            }
            fetch_accounts(current_slot, &mut accounts, &mut messages);

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
        accounts: Vec<ReplicaAccountInfo>,
    ) -> SqsClientResult {
        let entries = accounts
            .iter()
            .enumerate()
            .map(|(id, message)| SendMessageBatchRequestEntry {
                id: id.to_string(),
                message_body: message.serialize(),
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
                            let index = entry.id.parse::<usize>().ok();
                            let account = index.and_then(|index| accounts.get(index)).unwrap();

                            serde_json::json!({
                                "code": entry.code,
                                "message": entry.message,
                                "sender_fault": entry.sender_fault,
                                "pubkey": account.pubkey.to_string(),
                                "slot": account.slot,
                                "write_version": account.write_version,
                            })
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
