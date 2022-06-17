use {
    crate::config::ConfigPrometheus,
    futures::FutureExt,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    log::*,
    prometheus::{IntCounterVec, IntGauge, Opts, Registry, TextEncoder},
    std::sync::Once,
    tokio::{runtime::Runtime, sync::oneshot},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

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

#[derive(Debug, Clone, Copy)]
pub enum UploadMessagesStatus {
    Success,
    Failed,
    Dropped,
}

impl UploadMessagesStatus {
    pub fn as_str(&self) -> &str {
        match *self {
            UploadMessagesStatus::Success => "success",
            UploadMessagesStatus::Failed => "failed",
            UploadMessagesStatus::Dropped => "dropped",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UploadAwsStatus {
    Success,
    Failed,
}

impl UploadAwsStatus {
    pub fn as_str(&self) -> &str {
        match *self {
            UploadAwsStatus::Success => "success",
            UploadAwsStatus::Failed => "failed",
        }
    }
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown_signal: oneshot::Sender<()>,
}

impl PrometheusService {
    pub fn new(runtime: &Runtime, config: Option<ConfigPrometheus>) -> Self {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            macro_rules! register {
                ($collector:ident) => {
                    REGISTRY
                        .register(Box::new($collector.clone()))
                        .expect("collector can't be registered");
                };
            }
            register!(UPLOAD_MISSIED_INFO);
            register!(UPLOAD_QUEUE_SIZE);
            register!(UPLOAD_SQS_REQUESTS);
            register!(UPLOAD_SQS_TOTAL);
            register!(UPLOAD_S3_REQUESTS);
            register!(UPLOAD_S3_TOTAL);
        });

        let (tx, rx) = oneshot::channel();
        if let Some(ConfigPrometheus { address }) = config {
            runtime.spawn(async move {
                let make_service = make_service_fn(move |_: &AddrStream| async move {
                    Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| async move {
                        let response = match req.uri().path() {
                            "/metrics" => metrics_handler(),
                            _ => not_found_handler(),
                        };
                        Ok::<_, hyper::Error>(response)
                    }))
                });
                let server = Server::bind(&address).serve(make_service);
                if let Err(error) = tokio::try_join!(server, rx.map(|_| Ok(()))) {
                    error!("prometheus service failed: {}", error);
                }
            });
        }

        PrometheusService {
            shutdown_signal: tx,
        }
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_signal.send(());
    }
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder().body(Body::from(metrics)).unwrap()
}

fn not_found_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}
