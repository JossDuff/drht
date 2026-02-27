use anyhow::Result;
use clap::Parser;
use dht::KVPair;
use dht::{Config, LocalMessage, Node};
use rand::Rng;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{error, info};

// set the percentage of PUT and TRI_PUT operations.  Everything else is GET
const PUT_FREQUENCY: usize = 20;
const TRI_PUT_FREQUENCY: usize = 20;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // runtime for network layer
    let net_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("net")
        .enable_all()
        .build()?;

    // runtime for operations layer
    let ops_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("ops")
        .enable_all()
        .build()?;

    // handle is used to spawn tasks on this runtime
    let net_handle = net_rt.handle().clone();

    // start the application
    let handle = ops_rt.spawn(async move { run(net_handle).await });

    // wait for it to finish
    ops_rt.block_on(handle)??;
    Ok(())
}

async fn run(net_handle: tokio::runtime::Handle) -> Result<()> {
    let config = Config::parse();
    let num_keys = config.num_keys;
    let key_range = config.key_range;

    info!("I am {}", config.name);
    info!("I will connect to {:?}", config.connections);

    // make initial connections
    // net_handle gets passed through this into the networking task
    let (node, sender) = Node::<u64, u8>::new(config, &net_handle).await?;

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
    let test_data = generate_test_operations(num_keys, key_range);

    // Give the node time to start its event loop
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    info!("Starting test");
    let start = Instant::now();
    let mut handles = Vec::with_capacity(test_data.len());

    for i in 0..test_data.len() {
        print_progress(i, test_data.len());
        let operation = test_data[i];
        let sender = sender.clone();
        handles.push(tokio::spawn(async move {
            let req_start = Instant::now();
            match operation {
                Operation::Get { key } => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Get {
                        key,
                        response_sender,
                    };

                    if let Err(e) = sender.send(message).await {
                        error!("Error sending operation {:?}: {e}", operation);
                    }
                    if let Err(e) = response_receiver.await {
                        error!(
                            "Error receiving response for operation {:?}: {e}",
                            operation
                        );
                    }
                }
                Operation::Put { pair } => {
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::Put {
                        pair,
                        response_sender,
                    };

                    if let Err(e) = sender.send(message).await {
                        error!("Error sending operation {:?}: {e}", operation);
                    }
                    if let Err(e) = response_receiver.await {
                        error!(
                            "Error receiving response for operation {:?}: {e}",
                            operation
                        );
                    }
                }
                Operation::TriPut { pairs } => {
                    info!("Starting TriPut");
                    let tri_start = Instant::now();
                    let (response_sender, response_receiver) = oneshot::channel();
                    let message = LocalMessage::TriPut {
                        pairs,
                        response_sender,
                    };

                    if let Err(e) = sender.send(message).await {
                        error!("Error sending operation {:?}: {e}", operation);
                    }
                    if let Err(e) = response_receiver.await {
                        error!(
                            "Error receiving response for operation {:?}: {e}",
                            operation
                        );
                    }
                    let tri_end_ms = tri_start.elapsed().as_millis();
                    info!("TriPut took {}ms", tri_end_ms);
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

    // Signal peers we're done
    sender.send(LocalMessage::Done).await?;

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

    node_handle.await?;

    std::process::exit(0);
}

fn generate_test_operations(num_operations: usize, key_range: u64) -> Vec<Operation> {
    let mut rng = rand::rng();
    let total = PUT_FREQUENCY + TRI_PUT_FREQUENCY; // everything else is get

    (0..num_operations)
        .map(|_| {
            let roll = rng.random_range(0..100);
            if roll < PUT_FREQUENCY {
                Operation::Put {
                    pair: KVPair {
                        key: rng.random_range(0..key_range),
                        val: rng.random(),
                    },
                }
            } else if roll < total {
                Operation::TriPut {
                    pairs: [
                        KVPair {
                            key: rng.random_range(0..key_range),
                            val: rng.random(),
                        },
                        KVPair {
                            key: rng.random_range(0..key_range),
                            val: rng.random(),
                        },
                        KVPair {
                            key: rng.random_range(0..key_range),
                            val: rng.random(),
                        },
                    ],
                }
            } else {
                Operation::Get {
                    key: rng.random_range(0..key_range),
                }
            }
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Get { key: u64 },
    Put { pair: KVPair<u64, u8> },
    TriPut { pairs: [KVPair<u64, u8>; 3] },
}

fn print_progress(i: usize, total: usize) {
    let step = total / 10;
    if step > 0 && i % step == 0 {
        info!("{}% complete", (i * 100) / total);
    }
}
