use {
    crate::{config::ConfigPrometheus, version::VERSION as VERSION_INFO},
    http_body_util::{combinators::BoxBody, BodyExt, Empty as BodyEmpty, Full as BodyFull},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    log::*,
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder},
    std::{
        convert::Infallible,
        sync::{Arc, Once},
    },
    tokio::{net::TcpListener, sync::Notify},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "rustc", "solana", "version"]
    ).unwrap();

    static ref HEALTH_INFO: IntGaugeVec = IntGaugeVec::new(
        Opts::new("health_info", "Plugin health info, 0 is err, 1 is ok"),
        &["type"]
    ).unwrap();

    pub static ref SLOTS_LAST_PROCESSED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slots_last_processed", "Last processed slot by plugin in send loop"),
        &["status"]
    ).unwrap();

    pub static ref UPLOAD_MISSIED_INFO: IntGauge = IntGauge::new(
        "upload_missied_info",
        "Number of slots with missed info (accounts/transactions/block)"
    ).unwrap();

    pub static ref UPLOAD_QUEUE_SIZE: IntGauge = IntGauge::new(
        "upload_queue_size",
        "Number of messages in the queue for upload"
    ).unwrap();

    pub static ref UPLOAD_MESSAGES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("upload_messages_total", "Status of uploaded messages"),
        &["status"]
    ).unwrap();

    pub static ref UPLOAD_SQS_REQUESTS: IntGauge = IntGauge::new(
        "upload_sqs_requests",
        "Number of active upload SQS requests"
    ).unwrap();

    pub static ref UPLOAD_SQS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("upload_sqs_total", "Status of uploaded SQS messages"),
        &["status"]
    ).unwrap();

    pub static ref UPLOAD_S3_REQUESTS: IntGauge = IntGauge::new(
        "upload_s3_requests",
        "Number of active upload S3 requests"
    ).unwrap();

    pub static ref UPLOAD_S3_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("upload_s3_total", "Status of uploaded S3 payloads"),
        &["status"]
    ).unwrap();
}

pub mod health {
    use super::HEALTH_INFO;

    #[derive(Debug, Clone, Copy)]
    pub enum HealthInfoType {
        SendLoop,
        RedisAdmin,
        RedisHeartbeat,
    }

    impl HealthInfoType {
        pub const fn as_str(self) -> &'static str {
            match self {
                Self::SendLoop => "send_loop",
                Self::RedisAdmin => "redis_admin",
                Self::RedisHeartbeat => "redis_heartbeat",
            }
        }
    }

    pub fn set_health(r#type: HealthInfoType, status: Result<(), ()>) {
        let value = if status.is_ok() { 1 } else { 0 };
        HEALTH_INFO.with_label_values(&[r#type.as_str()]).set(value);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UploadMessagesStatus {
    Success,
    Failed,
    Dropped,
}

impl UploadMessagesStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UploadAwsStatus {
    Success,
    Failed,
}

impl UploadAwsStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown: Arc<Notify>,
}

impl PrometheusService {
    pub async fn new(config: Option<ConfigPrometheus>) -> std::io::Result<Self> {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            macro_rules! register {
                ($collector:ident) => {
                    REGISTRY
                        .register(Box::new($collector.clone()))
                        .expect("collector can't be registered");
                };
            }
            register!(VERSION);
            register!(HEALTH_INFO);
            register!(SLOTS_LAST_PROCESSED);
            register!(UPLOAD_MISSIED_INFO);
            register!(UPLOAD_QUEUE_SIZE);
            register!(UPLOAD_SQS_REQUESTS);
            register!(UPLOAD_SQS_TOTAL);
            register!(UPLOAD_S3_REQUESTS);
            register!(UPLOAD_S3_TOTAL);

            VERSION
                .with_label_values(&[
                    VERSION_INFO.buildts,
                    VERSION_INFO.git,
                    VERSION_INFO.rustc,
                    VERSION_INFO.solana,
                    VERSION_INFO.version,
                ])
                .inc()
        });

        let shutdown = Arc::new(Notify::new());
        if let Some(ConfigPrometheus { address }) = config {
            let shutdown = Arc::clone(&shutdown);
            let listener = TcpListener::bind(&address).await?;
            tokio::spawn(async move {
                loop {
                    let stream = tokio::select! {
                        () = shutdown.notified() => break,
                        maybe_conn = listener.accept() => match maybe_conn {
                            Ok((stream, _addr)) => stream,
                            Err(error) => {
                                error!("failed to accept new connection: {error}");
                                break;
                            }
                        }
                    };

                    tokio::spawn(async move {
                        if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<BodyIncoming>| async move {
                                    match req.uri().path() {
                                        "/metrics" => metrics_handler(),
                                        _ => not_found_handler(),
                                    }
                                }),
                            )
                            .await
                        {
                            error!("failed to handle metrics request: {error}");
                        }
                    });
                }
            });
        }

        Ok(PrometheusService { shutdown })
    }

    pub fn shutdown(self) {
        self.shutdown.notify_one();
    }
}

fn metrics_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder()
        .status(StatusCode::OK)
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
}
