use super::NodeId;
use super::PeerMessage;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, hash::Hash, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tracing::{error, info};

const DOMAIN: &str = "cse.lehigh.edu";
const PORT: u64 = 1895;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadyPeerMessage(NodeId);

pub struct Peers<K: Clone, V: Clone> {
    pub inbox: mpsc::Receiver<(NodeId, PeerMessage<K, V>)>,
    pub senders: HashMap<NodeId, mpsc::Sender<PeerMessage<K, V>>>,
}

impl<K, V> Peers<K, V>
where
    K: Send + Sync + 'static + Hash + Clone,
    V: Send + Sync + 'static + Clone,
{
    // send message to single node
    pub async fn send(&self, to: &NodeId, msg: PeerMessage<K, V>) -> Result<()> {
        let sender = self
            .senders
            .get(to)
            .ok_or_else(|| anyhow!("unknown peer: {}", to))?;
        sender.send(msg).await?;
        Ok(())
    }
}

async fn writer_task<K, V>(
    peer_id: NodeId,
    mut write_half: tokio::net::tcp::OwnedWriteHalf,
    mut outbox: mpsc::Receiver<PeerMessage<K, V>>,
) where
    K: Serialize + for<'de> Deserialize<'de> + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    while let Some(msg) = outbox.recv().await {
        if let Err(e) = send_msg(&mut write_half, &msg).await {
            error!("[{}] Write error: {}", peer_id, e);
            break;
        }
    }
}

async fn reader_task<K, V>(
    peer_id: NodeId,
    mut read_half: tokio::net::tcp::OwnedReadHalf,
    inbox: mpsc::Sender<(NodeId, PeerMessage<K, V>)>,
) where
    K: Serialize + for<'de> Deserialize<'de> + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
{
    loop {
        match recv_msg(&mut read_half).await {
            Ok(msg) => {
                if inbox.send((peer_id.clone(), msg)).await.is_err() {
                    break; // Main task shut down
                }
            }
            Err(e) => {
                error!("[{}] Read error: {}", peer_id, e);
                break;
            }
        }
    }
}

// Handles serialization of the raw message
async fn send_msg<M, W>(stream: &mut W, msg: &M) -> Result<()>
where
    M: Serialize,
    W: AsyncWriteExt + Unpin,
{
    let encoded = bincode::serialize(msg)?;
    let len = encoded.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&encoded).await?;
    Ok(())
}

// handles deserialization of the raw message
async fn recv_msg<M, R>(stream: &mut R) -> Result<M>
where
    M: for<'de> Deserialize<'de>,
    R: AsyncReadExt + Unpin,
{
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > 10 * 1024 * 1024 {
        return Err(anyhow!("message too large"));
    }
    let mut buffer = vec![0u8; len];
    stream.read_exact(&mut buffer).await?;
    Ok(bincode::deserialize(&buffer)?)
}

pub async fn connect_all<K, V>(
    my_name: &str,
    sunlab_nodes: &Vec<String>,
) -> Result<(Peers<K, V>, Vec<NodeId>, NodeId)>
where
    K: Serialize + for<'de> Deserialize<'de> + std::marker::Send + Sync + 'static + Clone,
    V: Serialize + for<'de> Deserialize<'de> + std::marker::Send + Sync + 'static + Clone,
{
    let listen_addr = format!("0.0.0.0:{PORT}");
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("[{}] Listening on {}", my_name, listen_addr);

    // Channel for completed connections (from both accept and connect paths)
    let (conn_sender, mut conn_receiver) = mpsc::channel::<(NodeId, TcpStream)>(64);

    // Spawn acceptor task
    let accept_sender = conn_sender.clone();
    let my_name_owned = my_name.to_string();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut stream, addr)) => {
                    let _ = stream.set_nodelay(true);

                    // Expect Ready message to identify peer
                    match recv_msg(&mut stream).await {
                        Ok(ReadyPeerMessage(from)) => {
                            info!(
                                "[{}] Accepted connection from {} ({})",
                                my_name_owned, from, addr
                            );
                            let _ = accept_sender.send((from, stream)).await;
                        }
                        Err(e) => error!("Failed to read Ready: {}", e),
                    }
                }
                Err(e) => error!("Accept failed: {}", e),
            }
        }
    });

    let my_name = my_name.to_string();
    let my_id = node_to_index(&my_name).ok_or(anyhow!(
        "This has an invalid sunlab machine name: {}",
        my_name
    ))?;
    let my_node_id = NodeId {
        sunlab_name: my_name.clone(),
        id: my_id,
    };

    // Spawn connect tasks for each peer
    for peer_name in sunlab_nodes {
        let peer_name = peer_name.clone();
        let sender = conn_sender.clone();
        let my_node_id = my_node_id.clone();
        let my_name = my_name.clone();

        tokio::spawn(async move {
            let peer_id = node_to_index(&peer_name).expect("invalid peer name");
            let peer_node_id = NodeId {
                sunlab_name: peer_name.clone(),
                id: peer_id,
            };

            match connect_with_retry(&peer_name, 5, Duration::from_secs(1)).await {
                Ok(mut stream) => {
                    let _ = stream.set_nodelay(true);

                    if let Err(e) =
                        send_msg(&mut stream, &ReadyPeerMessage(my_node_id.clone())).await
                    {
                        error!("[{}] Failed to send Ready to {}: {}", my_name, peer_name, e);
                        return;
                    }
                    info!("[{}] Connected to {}", my_name, peer_name);
                    let _ = sender.send((peer_node_id, stream)).await;
                }
                Err(e) => error!("[{}] Failed to connect to {}: {}", my_name, peer_name, e),
            }
        });
    }

    // Collect connections
    // waits until all connections are made
    let mut streams: HashMap<NodeId, TcpStream> = HashMap::new();
    while streams.len() < sunlab_nodes.len() {
        match conn_receiver.recv().await {
            Some((peer_id, stream)) => {
                streams.entry(peer_id).or_insert(stream);
            }
            None => break,
        }
    }

    info!("[{}] All {} peers connected", my_name, streams.len());

    // Now split each stream into reader/writer tasks with channels
    let (inbox_sender, inbox_receiver) = mpsc::channel::<(NodeId, PeerMessage<K, V>)>(256);
    let mut senders = HashMap::new();
    // list of all nodes in the cluster
    let mut cluster: Vec<NodeId> = streams.keys().cloned().collect();
    // include this node
    cluster.push(my_node_id.clone());
    // sort by id
    cluster.sort_by_key(|x| x.id);

    for (peer_id, stream) in streams {
        let (read_half, write_half) = stream.into_split();
        let (outbox_sender, outbox_receiver) = mpsc::channel::<PeerMessage<K, V>>(64);

        // Reader task: recv from socket -> inbox
        let inbox_sender = inbox_sender.clone();
        let peer_id_clone = peer_id.clone();
        tokio::spawn(async move { reader_task(peer_id_clone, read_half, inbox_sender).await });

        // Writer task: outbox -> send to socket
        let peer_id_clone = peer_id.clone();
        tokio::spawn(async move { writer_task(peer_id_clone, write_half, outbox_receiver).await });

        senders.insert(peer_id, outbox_sender);
    }

    Ok((
        Peers {
            inbox: inbox_receiver,
            senders,
        },
        cluster,
        my_node_id,
    ))
}

async fn connect_with_retry(
    node_name: &str,
    max_attempts: u32,
    delay: Duration,
) -> Result<TcpStream> {
    let addr = get_connect_str(node_name);
    let mut attempts = 0;

    loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(e.into());
                }
                error!(
                    "Connection to {} failed (attempt {}), retrying...",
                    addr, attempts
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

// all possible sunlab nodes, their id is their position in this array
fn node_to_index(name: &str) -> Option<usize> {
    const NODES: &[&str] = &[
        "ariel", "caliban", "callisto", "ceres", "chiron", "cupid", "eris", "europa", "hydra",
        "iapetus", "io", "ixion", "mars", "mercury", "neptune", "nereid", "nix", "orcus", "phobos",
        "puck", "saturn", "triton", "varda", "vesta", "xena",
    ];
    NODES.iter().position(|&n| n == name)
}

fn get_connect_str(name: &str) -> String {
    format!("{name}.{DOMAIN}:{PORT}")
}

#[cfg(test)]
mod tests {
    use super::get_connect_str;

    #[test]
    fn test_connection_string_parsing() {
        let correct = "io.cse.lehigh.edu:1895";
        let actual = get_connect_str("io");

        assert_eq!(correct, actual);
    }
}
