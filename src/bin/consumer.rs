use {
    anyhow::Result,
    clap::Parser,
    futures::stream::{self, StreamExt},
    humantime::format_rfc3339_millis,
    rusoto_s3::{DeleteObjectRequest, GetObjectOutput, GetObjectRequest, PutObjectRequest, S3},
    rusoto_sqs::{
        DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, ReceiveMessageRequest,
        ReceiveMessageResult, SendMessageBatchRequest, SendMessageBatchRequestEntry, Sqs,
    },
    serde::Deserialize,
    solana_geyser_sqs::{
        aws::{S3Client, SqsClient, SqsMessageAttributes},
        config::{AccountsDataCompression, Config},
    },
    std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        time::SystemTime,
    },
    tokio::{
        io::AsyncReadExt,
        time::{sleep, Duration},
    },
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Path to geyser plugin config
    #[clap(short, long)]
    config: String,

    /// Upload payloads and send messages
    #[clap(short, long)]
    generate: bool,

    /// Print full messages
    #[clap(short, long)]
    details: bool,

    /// Max number of messages per one request
    #[clap(short, long, default_value_t = 10)]
    max_messages: i64,
}

#[derive(Debug, Deserialize)]
struct Message<'a> {
    pub pubkey: &'a str,
    pub owner: &'a str,
    pub filters: Vec<&'a str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)?;

    if args.generate {
        let config = config.clone();
        tokio::spawn(async move {
            send_loop(config).await.unwrap();
        });
    }

    receive_loop(args, config).await
}

async fn send_loop(config: Config) -> anyhow::Result<()> {
    let sqs = SqsClient::new(config.sqs.clone())?;
    let s3 = S3Client::new(config.s3.clone())?;

    loop {
        let body = "hello world".as_bytes().to_vec();
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        let key = format!("consumer-test-{:x}", hasher.finish());

        let result = s3
            .client
            .put_object(PutObjectRequest {
                body: Some(body.into()),
                bucket: s3.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            })
            .await;
        println!("Put s3 object ({}): {:?}", key, result);

        let mut attributes = SqsMessageAttributes::default();
        attributes.insert("compression", AccountsDataCompression::None.as_str());

        let result = sqs
            .client
            .send_message_batch(SendMessageBatchRequest {
                entries: vec![SendMessageBatchRequestEntry {
                    id: "0".to_owned(),
                    message_body: serde_json::json!({ "s3": key }).to_string(),
                    message_attributes: Some(attributes.into_inner()),
                    ..Default::default()
                }],
                queue_url: sqs.queue_url.clone(),
            })
            .await;
        println!("Send sqs message: {:?}", result);

        sleep(Duration::from_secs(1)).await;
        // break Ok(());
    }
}

async fn receive_loop(args: Args, config: Config) -> anyhow::Result<()> {
    let sqs = SqsClient::new(config.sqs)?;
    let s3 = S3Client::new(config.s3)?;

    loop {
        let result = sqs
            .client
            .receive_message(ReceiveMessageRequest {
                max_number_of_messages: Some(args.max_messages),
                message_attribute_names: Some(vec!["All".to_owned()]),
                queue_url: sqs.queue_url.clone(),
                ..Default::default()
            })
            .await;
        let now = format_rfc3339_millis(SystemTime::now());
        match result {
            Ok(ReceiveMessageResult {
                messages: Some(messages),
            }) => {
                let messages = stream::iter(messages)
                    .filter_map(|mut message| {
                        let s3 = message
                            .body
                            .as_ref()
                            .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
                            .and_then(|value| {
                                value
                                    .get("s3")
                                    .and_then(|value| value.as_str().map(|s| s.to_owned()))
                            })
                            .map(|key| (s3.clone(), key));

                        async move {
                            if let Some((s3, key)) = s3 {
                                let request = GetObjectRequest {
                                    bucket: s3.bucket.clone(),
                                    key: key.clone(),
                                    ..Default::default()
                                };
                                match s3.client.get_object(request).await {
                                    Ok(GetObjectOutput {
                                        body: Some(body), ..
                                    }) => {
                                        let mut body = body.into_async_read();
                                        let mut payload = String::new();
                                        if body.read_to_string(&mut payload).await.is_ok() {
                                            message.body = Some(payload);
                                        }
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        println!(
                                            "failed to get payload from s3 ({}): {:?}",
                                            key, error
                                        )
                                    }
                                };

                                let request = DeleteObjectRequest {
                                    bucket: s3.bucket.clone(),
                                    key: key.clone(),
                                    ..Default::default()
                                };
                                if let Err(error) = s3.client.delete_object(request).await {
                                    println!(
                                        "failed to delete payload from s3 ({}): {:?}",
                                        key, error
                                    );
                                }
                            }
                            Some(message)
                        }
                    })
                    .collect::<Vec<_>>()
                    .await;

                if args.details {
                    println!("{} | messages: {:?}", now, messages);
                } else {
                    let messages = messages
                        .iter()
                        .filter_map(|message| {
                            message.body.as_ref().map(|body| {
                                let msg = serde_json::from_str::<Message>(body).unwrap();
                                format!(
                                    "{} (owner: {}) => {:?}",
                                    msg.pubkey, msg.owner, msg.filters
                                )
                            })
                        })
                        .collect::<Vec<_>>();
                    println!("{} | messages: [{}]", now, messages.join(", "));
                }
                let entries = messages
                    .into_iter()
                    .enumerate()
                    .filter_map(|(id, message)| {
                        message.receipt_handle.map(|receipt_handle| {
                            DeleteMessageBatchRequestEntry {
                                id: id.to_string(),
                                receipt_handle,
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                if !entries.is_empty() {
                    let _ = sqs
                        .client
                        .delete_message_batch(DeleteMessageBatchRequest {
                            entries,
                            queue_url: sqs.queue_url.clone(),
                        })
                        .await;
                }
            }
            Ok(ReceiveMessageResult { messages: None }) => sleep(Duration::from_millis(500)).await,
            Err(error) => println!("{} | error: {:?}", now, error),
        };
    }
}
