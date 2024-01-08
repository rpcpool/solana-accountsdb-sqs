use {
    crate::{
        config::{ConfigAwsAuth, ConfigAwsS3, ConfigAwsSqs},
        prom::{
            UploadAwsStatus, UPLOAD_S3_REQUESTS, UPLOAD_S3_TOTAL, UPLOAD_SQS_REQUESTS,
            UPLOAD_SQS_TOTAL,
        },
        sqs::SendMessageType,
    },
    futures::future::{try_join_all, BoxFuture},
    hyper::Client,
    hyper_tls::HttpsConnector,
    log::*,
    rusoto_core::{request::TlsError, ByteStream, Client as RusotoClient, HttpClient, RusotoError},
    rusoto_credential::{
        AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProfileProvider,
        ProvideAwsCredentials, StaticProvider,
    },
    rusoto_s3::{PutObjectError, PutObjectRequest, S3Client as RusotoS3Client, S3},
    rusoto_sqs::{
        BatchResultErrorEntry, GetQueueAttributesError, GetQueueAttributesRequest,
        MessageAttributeValue, SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs,
        SqsClient as RusotoSqsClient,
    },
    serde::de::{self, Deserialize, Deserializer},
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    },
    thiserror::Error,
    tokio::{sync::Semaphore, time::sleep},
};

#[derive(Debug, Error)]
pub enum AwsError {
    #[error("credential error: {0}")]
    Credentials(#[from] CredentialsError),
    #[error("http client error: {0}")]
    HttpClientTls(#[from] TlsError),
    #[error("failed to get sqs queue attributes: {0}")]
    SqsGetAttributes(#[from] RusotoError<GetQueueAttributesError>),
    #[error("S3 key is not valid UTF-8 string")]
    S3Path,
    #[error("failed to upload payload to s3: {0}")]
    S3PutObject(#[from] RusotoError<PutObjectError>),
}

pub type AwsResult<T = ()> = Result<T, AwsError>;

#[derive(Debug, Clone, Default)]
pub struct SqsMessageAttributes {
    map: HashMap<String, MessageAttributeValue>,
}

impl SqsMessageAttributes {
    pub fn insert<S1: Into<String>, S2: Into<String>>(&mut self, key: S1, value: S2) -> &mut Self {
        self.map.insert(
            key.into(),
            MessageAttributeValue {
                data_type: "String".to_owned(),
                string_value: Some(value.into()),
                ..Default::default()
            },
        );
        self
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map
            .get(key)
            .and_then(|attr| attr.string_value.as_ref())
            .map(|s| s.as_str())
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn into_inner(self) -> HashMap<String, MessageAttributeValue> {
        self.map
    }
}

impl<'de> Deserialize<'de> for SqsMessageAttributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let not_allowed = ["compression", "md5", "node"];

        let mut attributes = SqsMessageAttributes::default();

        let map = HashMap::<String, String>::deserialize(deserializer)?;
        for (attribute, value) in map.into_iter() {
            if not_allowed.contains(&attribute.as_str()) {
                return Err(de::Error::custom(format!(
                    "key {:?} is not allowed as attribute",
                    attribute
                )));
            }
            attributes.insert(attribute, value);
        }

        Ok(attributes)
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum SqsClientQueueUrl {
    Shared(String),
    Typed {
        slots: String,
        accounts: String,
        transactions: String,
    },
}

impl SqsClientQueueUrl {
    pub fn optimize(self) -> Self {
        match self {
            Self::Shared(url) => Self::Shared(url),
            Self::Typed {
                slots,
                accounts,
                transactions,
            } if slots == accounts && accounts == transactions => Self::Shared(slots),
            Self::Typed {
                slots,
                accounts,
                transactions,
            } => Self::Typed {
                slots,
                accounts,
                transactions,
            },
        }
    }

    pub fn get_urls(self) -> Vec<String> {
        match self {
            Self::Shared(url) => vec![url],
            Self::Typed {
                slots,
                accounts,
                transactions,
            } => vec![slots, accounts, transactions],
        }
    }
}

#[derive(derivative::Derivative)]
#[derivative(Debug, Clone)]
pub struct SqsClient {
    #[derivative(Debug = "ignore")]
    pub client: RusotoSqsClient,
    pub queue_url: SqsClientQueueUrl,
    attributes: SqsMessageAttributes,
}

impl SqsClient {
    // The maximum allowed individual message size and the maximum total payload size (the sum of the
    // individual lengths of all of the batched messages) are both 256 KB (262,144 bytes).
    pub const REQUEST_LIMIT: usize = 250_000;

    pub fn new<S: Into<String>>(mut config: ConfigAwsSqs, node: S) -> AwsResult<Self> {
        let client = aws_create_client(config.auth)?;
        config.attributes.insert("node", node.into());
        Ok(Self {
            client: RusotoSqsClient::new_with_client(client, config.region),
            queue_url: config.url.optimize(),
            attributes: config.attributes,
        })
    }

    pub async fn check(self) -> AwsResult {
        let client = self.client.clone();
        try_join_all(
            self.queue_url
                .get_urls()
                .into_iter()
                .map(|queue_url| {
                    let client = client.clone();
                    async move {
                        UPLOAD_SQS_REQUESTS.inc();
                        let result = client
                            .get_queue_attributes(GetQueueAttributesRequest {
                                attribute_names: None,
                                queue_url,
                            })
                            .await;
                        UPLOAD_SQS_REQUESTS.dec();

                        result
                    }
                })
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(AwsError::SqsGetAttributes)
        .map(|_| ())
    }

    pub const fn is_typed(&self) -> bool {
        matches!(self.queue_url, SqsClientQueueUrl::Typed { .. })
    }

    pub fn get_attributes(&self) -> SqsMessageAttributes {
        self.attributes.clone()
    }

    pub async fn send_batch(
        self,
        mut entries: Vec<SendMessageBatchRequestEntry>,
        messages_type: Option<SendMessageType>,
    ) -> Vec<BatchResultErrorEntry> {
        let Self {
            client, queue_url, ..
        } = self;

        let queue_url = match (queue_url, messages_type) {
            (SqsClientQueueUrl::Shared(url), _) => Some(url),
            (
                SqsClientQueueUrl::Typed {
                    slots,
                    accounts,
                    transactions,
                },
                Some(messages_type),
            ) => Some(match messages_type {
                SendMessageType::Slot => slots,
                SendMessageType::Account => accounts,
                SendMessageType::Transaction => transactions,
            }),
            (SqsClientQueueUrl::Typed { .. }, None) => None,
        };

        let result = match queue_url {
            Some(queue_url) => {
                if queue_url.ends_with(".fifo") {
                    for entry in entries.iter_mut() {
                        entry.message_group_id = Some("solana".to_owned());
                    }
                }

                UPLOAD_SQS_REQUESTS.inc();
                let result = with_retries("sqs", ExponentialBackoff::default(), || {
                    let input = SendMessageBatchRequest {
                        entries: entries.clone(),
                        queue_url: queue_url.clone(),
                    };
                    let client = client.clone();
                    Box::pin(async move { client.send_message_batch(input).await })
                })
                .await;
                UPLOAD_SQS_REQUESTS.dec();
                result.map_err(|_error| ())
            }
            None => Err(()),
        };

        let failed = match result {
            Ok(rusoto_sqs::SendMessageBatchResult { successful, failed }) => {
                if !successful.is_empty() {
                    UPLOAD_SQS_TOTAL
                        .with_label_values(&[UploadAwsStatus::Success.as_str()])
                        .inc_by(successful.len() as u64);
                }
                failed
            }
            Err(_error) => (0..entries.len())
                .map(|id| BatchResultErrorEntry {
                    id: id.to_string(),
                    ..Default::default()
                })
                .collect(),
        };
        if !failed.is_empty() {
            UPLOAD_SQS_TOTAL
                .with_label_values(&[UploadAwsStatus::Failed.as_str()])
                .inc_by(failed.len() as u64);
        }
        failed
    }
}

#[derive(derivative::Derivative)]
#[derivative(Debug, Clone)]
pub struct S3Client {
    #[derivative(Debug = "ignore")]
    pub client: RusotoS3Client,
    pub bucket: String,
    pub prefix: PathBuf,
    pub permits: Arc<Semaphore>,
}

impl S3Client {
    pub fn new(config: ConfigAwsS3) -> AwsResult<Self> {
        let client = aws_create_client(config.auth)?;
        Ok(Self {
            client: RusotoS3Client::new_with_client(client, config.region),
            bucket: config.bucket,
            prefix: config.prefix,
            permits: Arc::new(Semaphore::new(config.max_requests)),
        })
    }

    pub async fn check(self) -> AwsResult {
        self.put_object("startup-check", vec![])
            .await
            .map_err(Into::into)
            .map(|_| ())
    }

    pub async fn put_object<K, B>(self, key: K, body: B) -> AwsResult
    where
        K: AsRef<Path> + Send,
        B: Into<ByteStream> + Clone + Send,
    {
        let mut prefix = self.prefix.clone();
        prefix.push(key);
        let key = match prefix.as_path().to_str() {
            Some(key) => key.to_string(),
            None => return Err(AwsError::S3Path),
        };

        let permit = self.permits.acquire().await.expect("alive");
        UPLOAD_S3_REQUESTS.inc();
        let result = with_retries("s3", ExponentialBackoff::default(), || {
            let input = PutObjectRequest {
                body: Some(body.clone().into()),
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            };
            let client = self.client.clone();
            Box::pin(async move { client.put_object(input).await })
        })
        .await;
        UPLOAD_S3_REQUESTS.dec();
        drop(permit);

        let status = match result {
            Ok(_) => UploadAwsStatus::Success,
            Err(_) => UploadAwsStatus::Failed,
        };
        UPLOAD_S3_TOTAL.with_label_values(&[status.as_str()]).inc();

        result.map_err(Into::into).map(|_| ())
    }
}

#[derive(Debug)]
struct ExponentialBackoff {
    current: Duration,
    retries: u8,
    factor: u32,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        // In milliseconds: 1 2 4 8 16
        ExponentialBackoff {
            current: Duration::from_millis(1),
            retries: 5,
            factor: 2,
        }
    }
}

impl Iterator for ExponentialBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if self.retries == 0 {
            return None;
        }
        self.retries -= 1;

        match (self.current, self.current.checked_mul(self.factor)) {
            (delay, Some(new_delay)) => {
                self.current = new_delay;
                Some(delay)
            }
            _ => None,
        }
    }
}

async fn with_retries<'a, T, E, F>(
    service: &str,
    mut backoff: ExponentialBackoff,
    make_request: F,
) -> Result<T, RusotoError<E>>
where
    F: Fn() -> BoxFuture<'a, Result<T, RusotoError<E>>>,
{
    let mut attempt = 0;
    loop {
        let next_delay = backoff.next();
        let (err_type, err_text) = match make_request().await {
            Ok(result) => break Ok(result),
            Err(RusotoError::HttpDispatch(res)) if next_delay.is_some() => {
                ("HTTP dispatch error", format!("{:?}", res))
            }
            Err(RusotoError::Unknown(res)) if next_delay.is_some() => {
                ("Internal Server Error", format!("{:?}", res))
            }
            Err(error) => break Err(error),
        };
        attempt += 1;
        warn!(
            "{} ({}, attempt: {}): {}",
            err_type, service, attempt, err_text
        );
        sleep(next_delay.unwrap()).await;
    }
}

fn aws_create_client(config: ConfigAwsAuth) -> AwsResult<RusotoClient> {
    let mut builder = Client::builder();
    builder.pool_idle_timeout(Duration::from_secs(10));
    builder.pool_max_idle_per_host(10);
    // Fix: `connection closed before message completed` but introduce `dns error: failed to lookup address information`
    // builder.pool_max_idle_per_host(0);
    let request_dispatcher = HttpClient::from_builder(builder, HttpsConnector::new());
    let credentials_provider = AwsCredentialsProvider::new(config)?;
    Ok(RusotoClient::new_with(
        credentials_provider,
        request_dispatcher,
    ))
}

#[allow(clippy::large_enum_variant)]
enum AwsCredentialsProvider {
    Static(StaticProvider),
    Chain(AutoRefreshingProvider<ChainProvider>),
}

impl AwsCredentialsProvider {
    pub fn new(config: ConfigAwsAuth) -> AwsResult<Self> {
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

#[async_trait::async_trait]
impl ProvideAwsCredentials for AwsCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        match self {
            Self::Static(p) => p.credentials(),
            Self::Chain(p) => p.credentials(),
        }
        .await
    }
}
