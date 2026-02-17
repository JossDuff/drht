use anyhow::Result;
use clap::Parser;
use dht::{Config, LocalMessage, Node};
use rand::Rng;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{error, info};

#[tokio::main(worker_threads = 3)]
async fn main() -> Result<()> {
    // init logger
    tracing_subscriber::fmt::init();

    let config = Config::parse();
    let num_keys = config.num_keys;
    let key_range = config.key_range;

    info!("I am {}", config.name);
    info!("I will connect to {:?}", config.connections);

    // make initial connections
    let (mut node, sender) = Node::<u64, u8>::new(config).await?;

    // run the node event handlers in their own task
    let node_handle = tokio::spawn(async move {
        if let Err(e) = node.run().await {
            error!("{e}");
        }
    });

    info!(
        "Generating {} key value pairs with range of {}",
        num_keys, key_range
    );
    let test_data = generate_test_data(num_keys, key_range);

    // Give the node time to start its event loop
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    info!("Starting test");
    let start = Instant::now();
    let mut handles = Vec::with_capacity(test_data.len());

    for data in test_data {
        let sender = sender.clone();
        handles.push(tokio::spawn(async move {
            let req_start = Instant::now();
            match data.operation {
                Operation::Get => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Get {
                        key: data.key,
                        response_sender,
                    };
                    if let Err(e) = sender.send(message).await {
                        error!("Error sending get operation {e}");
                    }
                    response_receiver.await.unwrap();
                }
                Operation::Put => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Put {
                        key: data.key,
                        val: data.val,
                        response_sender,
                    };
                    sender.send(message).await.unwrap();
                    response_receiver.await.unwrap();
                }
            }
            req_start.elapsed()
        }));
    }

    // Wait for all to complete
    let mut latencies: Vec<Duration> = Vec::with_capacity(handles.len());
    for handle in handles {
        latencies.push(handle.await?);
    }

    let elapsed = start.elapsed();
    info!("Completed {} operations in {:?}", num_keys, elapsed);
    info!(
        "Throughput: {:.2} ops/sec",
        num_keys as f64 / elapsed.as_secs_f64()
    );
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];
    let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    info!("Latency avg: {:?}, p50: {:?}, p99: {:?}", avg, p50, p99);

    let _ = node_handle.await?;

    // TODO: graceful, non-janky exit
    std::process::exit(0);
}

fn generate_test_data(num_keys: u64, key_range: u64) -> Vec<TestData> {
    let mut rng = rand::rng();
    (0..num_keys)
        .map(|_| TestData {
            key: rng.random_range(0..key_range),
            val: rng.random(),
            operation: if rng.random_bool(0.8) {
                Operation::Get
            } else {
                Operation::Put
            },
        })
        .collect()
}

struct TestData {
    key: u64,
    val: u8,
    operation: Operation,
}

enum Operation {
    Get,
    Put,
}
