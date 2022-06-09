use {
    crate::{
        config::{ConfigAwsAuth, ConfigAwsS3, ConfigAwsSqs},
        prom::{
            UploadAwsStatus, UPLOAD_S3_REQUESTS, UPLOAD_S3_TOTAL, UPLOAD_SQS_REQUESTS,
            UPLOAD_SQS_TOTAL,
        },
    },
    http::StatusCode,
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
        MessageAttributeValue, SendMessageBatchError, SendMessageBatchRequest,
        SendMessageBatchRequestEntry, Sqs, SqsClient as RusotoSqsClient,
    },
    std::{collections::HashMap, sync::Arc, time::Duration},
    thiserror::Error,
    tokio::sync::Semaphore,
};

#[derive(Debug, Error)]
pub enum AwsError {
    #[error("credential error: {0}")]
    Credentials(#[from] CredentialsError),
    #[error("http client error: {0}")]
    HttpClientTls(#[from] TlsError),
    #[error("failed to get sqs queue attributes: {0}")]
    SqsGetAttributes(#[from] RusotoError<GetQueueAttributesError>),
    #[error("failed to send messages to sqs: {0}")]
    SqsSendMessageBatch(#[from] RusotoError<SendMessageBatchError>),
    #[error("failed to upload payload to s3: {0}")]
    S3PutObject(#[from] RusotoError<PutObjectError>),
}

pub type AwsResult<T = ()> = Result<T, AwsError>;

#[derive(Debug, Default)]
pub struct SqsMessageAttributes {
    map: HashMap<String, MessageAttributeValue>,
}

impl SqsMessageAttributes {
    pub fn new<S1: Into<String>, S2: Into<String>>(key: S1, value: S2) -> Self {
        let mut attributes = Self::default();
        attributes.insert(key, value);
        attributes
    }

    pub fn insert<S1: Into<String>, S2: Into<String>>(&mut self, key: S1, value: S2) -> &Self {
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

    pub fn into_inner(self) -> HashMap<String, MessageAttributeValue> {
        self.map
    }
}

#[derive(derivative::Derivative)]
#[derivative(Debug, Clone)]
pub struct SqsClient {
    #[derivative(Debug = "ignore")]
    pub client: RusotoSqsClient,
    pub queue_url: String,
}

impl SqsClient {
    // The maximum allowed individual message size and the maximum total payload size (the sum of the
    // individual lengths of all of the batched messages) are both 256 KB (262,144 bytes).
    pub const REQUEST_LIMIT: usize = 250_000;

    pub fn new(config: ConfigAwsSqs) -> AwsResult<Self> {
        let client = aws_create_client(config.auth)?;
        Ok(Self {
            client: RusotoSqsClient::new_with_client(client, config.region),
            queue_url: config.url,
        })
    }

    pub async fn check(self) -> AwsResult {
        UPLOAD_SQS_REQUESTS.inc();
        let result = self
            .client
            .get_queue_attributes(GetQueueAttributesRequest {
                attribute_names: None,
                queue_url: self.queue_url,
            })
            .await;
        UPLOAD_SQS_REQUESTS.dec();

        result.map_err(Into::into).map(|_| ())
    }

    pub async fn send_batch(
        self,
        entries: Vec<SendMessageBatchRequestEntry>,
    ) -> Vec<BatchResultErrorEntry> {
        let entries_count = entries.len();

        UPLOAD_SQS_REQUESTS.inc();
        let result = self
            .client
            .send_message_batch(SendMessageBatchRequest {
                entries,
                queue_url: self.queue_url,
            })
            .await;
        UPLOAD_SQS_REQUESTS.dec();

        let failed = match result {
            Ok(rusoto_sqs::SendMessageBatchResult { successful, failed }) => {
                if !successful.is_empty() {
                    UPLOAD_SQS_TOTAL
                        .with_label_values(&[UploadAwsStatus::Success.as_str()])
                        .inc_by(successful.len() as u64);
                }
                failed
            }
            Err(_error) => (0..entries_count)
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
    pub permits: Arc<Semaphore>,
}

impl S3Client {
    pub fn new(config: ConfigAwsS3) -> AwsResult<Self> {
        let client = aws_create_client(config.auth)?;
        Ok(Self {
            client: RusotoS3Client::new_with_client(client, config.region),
            bucket: config.bucket,
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
        K: Into<String> + Clone,
        B: Into<ByteStream> + Clone,
    {
        const HTTP_DISPATCH_CONNECTION_CLOSED: &str =
            "Error during dispatch: connection closed before message completed";
        const HTTP_DISPATCH_CONNECTION_TIMEDOUT: &str =
            "Error during dispatch: error trying to connect: tcp connect error: Connection timed out (os error 110)";

        let permit = self.permits.acquire().await.expect("alive");
        UPLOAD_S3_REQUESTS.inc();
        let mut retries = 3;
        let result = loop {
            let input = PutObjectRequest {
                body: Some(body.clone().into()),
                bucket: self.bucket.clone(),
                key: key.clone().into(),
                ..Default::default()
            };
            match self.client.put_object(input).await {
                Ok(result) => break Ok(result),
                Err(RusotoError::HttpDispatch(res))
                    if retries > 0
                        && matches!(
                            res.to_string().as_str(),
                            HTTP_DISPATCH_CONNECTION_CLOSED | HTTP_DISPATCH_CONNECTION_TIMEDOUT
                        ) =>
                {
                    retries -= 1;
                    warn!(
                        "HTTP dispatch error (remained retries: {}): {:?}",
                        retries, res
                    );
                }
                Err(RusotoError::Unknown(res))
                    if retries > 0 && res.status == StatusCode::INTERNAL_SERVER_ERROR =>
                {
                    retries -= 1;
                    warn!(
                        "Internal Server Error (remained retries: {}): {:?}",
                        retries, res
                    );
                }
                Err(error) => break Err(error),
            }
        };
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
