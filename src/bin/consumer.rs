use {
    anyhow::Result,
    clap::{crate_name, App, Arg},
    humantime::format_rfc3339_millis,
    rusoto_sqs::{
        DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry, ReceiveMessageRequest,
        ReceiveMessageResult, Sqs,
    },
    solana_accountsdb_sqs::{config::Config, sqs::AwsSqsClient},
    std::time::SystemTime,
    tokio::time::{sleep, Duration},
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = App::new(crate_name!())
        .arg(
            Arg::with_name("config")
                .help("Path to accountsdb plugin config")
                .long("config")
                .short("c")
                .required(true)
                .takes_value(true),
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
                println!("{} | messages: {:?}", now, messages);
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
