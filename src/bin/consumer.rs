use {
    anyhow::Result,
    clap::{crate_name, Arg, Command},
    humantime::format_rfc3339_millis,
    rusoto_sqs::{
        DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, ReceiveMessageRequest,
        ReceiveMessageResult, Sqs,
    },
    serde::Deserialize,
    solana_geyser_sqs::{config::Config, sqs::AwsSqsClient},
    std::time::SystemTime,
    tokio::time::{sleep, Duration},
};

#[derive(Debug, Deserialize)]
struct Message<'a> {
    pub pubkey: &'a str,
    pub owner: &'a str,
    pub filters: Vec<&'a str>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::new(crate_name!())
        .arg(
            Arg::new("config")
                .help("Path to geyser plugin config")
                .long("config")
                .short('c')
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("details")
                .help("Print full messages")
                .long("details")
                .short('d')
                .takes_value(false),
        )
        .get_matches();

    let config_path = args.value_of("config").unwrap();
    let config = Config::load_from_file(config_path)?;

    // let (client, queue_url) = AwsSqsClient::create_sqs(config.sqs.clone())?;
    // tokio::spawn(async move {
    //     use rusoto_sqs::{SendMessageBatchRequest, SendMessageBatchRequestEntry};
    //     loop {
    //         let result = client
    //             .send_message_batch(SendMessageBatchRequest {
    //                 entries: vec![SendMessageBatchRequestEntry {
    //                     id: "0".to_owned(),
    //                     message_body: "hello world".to_owned(),
    //                     ..Default::default()
    //                 }],
    //                 queue_url: queue_url.clone(),
    //             })
    //             .await;
    //         println!("{:?}", result);
    //         sleep(Duration::from_secs(1)).await;
    //     }
    // });

    let (client, queue_url) = AwsSqsClient::create_sqs(config.sqs)?;
    loop {
        let result = client
            .receive_message(ReceiveMessageRequest {
                max_number_of_messages: Some(10),
                queue_url: queue_url.clone(),
                ..Default::default()
            })
            .await;
        let now = format_rfc3339_millis(SystemTime::now());
        match result {
            Ok(ReceiveMessageResult {
                messages: Some(messages),
            }) => {
                if args.is_present("details") {
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
                    let _ = client
                        .delete_message_batch(DeleteMessageBatchRequest {
                            entries,
                            queue_url: queue_url.clone(),
                        })
                        .await;
                }
            }
            Ok(ReceiveMessageResult { messages: None }) => sleep(Duration::from_millis(500)).await,
            Err(error) => println!("{} | error: {:?}", now, error),
        };
    }
}
