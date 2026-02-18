mod config;
mod db;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use db::StripedDb;
use net::{connect_all, Peers};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error, info};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage<K, V> {
    Get { key: K, req_id: u64 },
    GetResponse { val: Option<V>, req_id: u64 },
    Put { key: K, val: V, req_id: u64 },
    PutResponse { success: bool, req_id: u64 },
    Done,
}

#[derive(Debug)]
pub enum LocalMessage<K, V: Clone> {
    Get {
        key: K,
        response_sender: oneshot::Sender<Option<V>>,
    },
    Put {
        key: K,
        val: V,
        response_sender: oneshot::Sender<bool>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct NodeId {
    sunlab_name: String,
    id: usize,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.sunlab_name, self.id)
    }
}

pub struct Node<K, V: Clone> {
    peers: Peers<K, V>,
    // all the nodes in this cluster, in consistent ordering
    cluster: Vec<NodeId>,
    // number of nodes that have finished their test
    done_count: AtomicUsize,
    // how many nodes hold each key
    replication_degree: usize,
    my_node_id: NodeId,
    local_inbox: mpsc::Receiver<LocalMessage<K, V>>,
    db: Arc<StripedDb<K, V>>,
    awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>>,
}

impl<K, V> Node<K, V>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub async fn new(config: Config) -> Result<(Self, mpsc::Sender<LocalMessage<K, V>>)> {
        let db = Arc::new(StripedDb::new(config.stripes));
        let awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // this is a barrier
        let (peers, cluster, my_node_id): (Peers<K, V>, Vec<NodeId>, NodeId) =
            connect_all::<K, V>(&config.name, &config.connections).await?;

        // for sending/ receiving messages from the test harness
        let (local_sender, local_inbox) = mpsc::channel(64);

        let done_count = AtomicUsize::new(0);

        Ok((
            Self {
                peers,
                cluster,
                done_count,
                my_node_id,
                local_inbox,
                replication_degree: config.repication_degree,
                awaiting_get_response,
                awaiting_put_response,
                db,
            },
            local_sender,
        ))
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            // using tokio select because we expect to be io bound, not cpu bound
            // TODO: make these two separate tasks
            tokio::select! {
                Some(local_msg) = self.local_inbox.recv() => {
                    self.handle_local_message(local_msg).await?;
                }
                Some((from, peer_msg)) = self.peers.inbox.recv() => {
                    // returns true when all peers are done
                    if self.handle_peer_message(from, peer_msg).await? {
                        info!("All peers done, shutting down");
                        break;
                    }
                }
                else => {
                    info!("All channels closed");
                    break;
                }
            }
        }
        Ok(())
    }

    // handles messages from the test harness and from the network loop when peers respond
    async fn handle_local_message(&self, msg: LocalMessage<K, V>) -> Result<()> {
        match msg {
            LocalMessage::Get {
                key,
                response_sender,
            } => {
                let owners = self.get_key_owners(&key, self.replication_degree);
                // key is locally owned, immedietly respond
                if self.is_owner(&key, self.replication_degree) {
                    let resp = self.local_get(&key).await;
                    response_sender
                        .send(resp)
                        .map_err(|_| anyhow!("Receiver for local get on key {:?} dropped", key))?;
                    return Ok(());
                }

                // just request one of the key's owners (assume replication is correct)
                let key_owner = owners
                    .first()
                    .ok_or(anyhow!("No owner found for key {:?}", key))?;

                let req_id: u64 = rand::rng().random();
                let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };
                self.peers.send(key_owner, request).await?;

                // create a task awaiting the response from a peer
                let mut awaiting_get_response = self.awaiting_get_response.lock().await;
                let (get_sender, get_receiver) = oneshot::channel();
                awaiting_get_response.insert(req_id, get_sender);

                // start a non-blocking task that awaits this response and sends back to test
                // harness
                tokio::spawn(async move {
                    match get_receiver.await {
                        Ok(peer_get_response) => {
                            // send get response back to test harness
                            let _ = response_sender.send(peer_get_response);
                        }
                        Err(e) => {
                            error!("Receive error waiting for getResponse: {}", e);
                        }
                    }
                });
            }
            LocalMessage::Put {
                key,
                val,
                response_sender,
            } => {
                let owners = self.get_key_owners(&key, self.replication_degree);
                // TODO: this is more complicated than Get
                // key is locally owned, immedietly respond
                if self.is_owner(&key, self.replication_degree) {
                    let resp = self.local_insert(key, val).await;
                    response_sender
                        .send(resp)
                        .map_err(|_| anyhow!("Receiver for local put on key {:?} dropped", key))?;
                    return Ok(());
                }

                for key_owner in owners {
                    // we need to request the key's owner
                    let req_id: u64 = rand::rng().random();
                    let request: PeerMessage<K, V> = PeerMessage::Put { key, req_id, val };
                    self.peers.send(key_owner, request).await?;

                    // create a task awaiting the response from a peer
                    let mut awaiting_put_response = self.awaiting_put_response.lock().await;
                    let (put_sender, put_receiver) = oneshot::channel();
                    awaiting_put_response.insert(req_id, put_sender);

                    // start a non-blocking task that awaits this response and sends back to test
                    // harness
                    tokio::spawn(async move {
                        match put_receiver.await {
                            Ok(peer_put_response) => {
                                // send get response back to test harness
                                let _ = response_sender.send(peer_put_response);
                            }
                            Err(e) => {
                                error!("Receive error waiting for putResponse: {}", e);
                            }
                        }
                    });
                }
            }
        }

        Ok(())
    }

    // handles messages from the network
    async fn handle_peer_message(&self, from: NodeId, msg: PeerMessage<K, V>) -> Result<bool> {
        debug!("Got {:?} from {}", msg, from);
        match msg {
            // peer is asking us for a get request
            PeerMessage::Get { key, req_id } => {
                let result = self.local_get(&key).await;
                let resp: PeerMessage<K, V> = PeerMessage::GetResponse {
                    val: result,
                    req_id,
                };

                self.peers
                    .send(&from, resp)
                    .await
                    .map_err(|e| anyhow!("Error sending GetResponse to node {}: {}", from, e))?;

                debug!(
                    "Sent GetResponse to {} for key {:?} req_id {}",
                    from, key, req_id
                );
            }
            // peer is asking us for a put request
            PeerMessage::Put { key, val, req_id } => {
                let result = self.local_insert(key, val).await;
                let resp: PeerMessage<K, V> = PeerMessage::PutResponse {
                    success: result,
                    req_id,
                };

                self.peers
                    .send(&from, resp)
                    .await
                    .map_err(|e| anyhow!("Error sending PutResponse to node {}: {}", from, e))?;

                debug!(
                    "Sent PutResponse to {} for key {:?} req_id {}",
                    from, key, req_id
                );
            }
            // received a response from a peer about a previous get request
            PeerMessage::GetResponse { val, req_id } => {
                // look up the channel for sending the response
                let mut awaiting_get_response = self.awaiting_get_response.lock().await;
                match awaiting_get_response.remove(&req_id) {
                    Some(sender) => {
                        // send the response to the local task awaiting it
                        sender.send(val).map_err(|_| {
                            anyhow!("Error sending local GetResponse for request {}", req_id)
                        })?;
                    }
                    None => {
                        return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                    }
                };
            }
            // received a response from a peer about a previous put request
            PeerMessage::PutResponse { success, req_id } => {
                // look up the channel for sending the response
                let mut awaiting_put_response = self.awaiting_put_response.lock().await;
                match awaiting_put_response.remove(&req_id) {
                    Some(sender) => {
                        // send the response for the local task awaiting it
                        sender.send(success).map_err(|_| {
                            anyhow!("Error sending local PutResponse for request {}", req_id)
                        })?;
                    }
                    None => {
                        return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                    }
                };
            }
            // a peer has finished their test
            PeerMessage::Done => {
                let count = self.done_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.cluster.len() - 1 {
                    // All peers done, we can exit
                    return Ok(true);
                }
            }
        }

        // keep running
        Ok(false)
    }

    async fn local_get(&self, key: &K) -> Option<V> {
        let db = self.db.lock().await;
        db.get(key).cloned()
    }

    async fn local_insert(&self, key: K, value: V) -> bool {
        let mut db = self.db.lock().await;

        match db.get(&key) {
            // an element already exists, return false
            Some(_) => false,
            // there is no element, insert and return true
            None => {
                let _ = db.insert(key, value);
                true
            }
        }
    }

    // maps the key to the owner nodes
    fn get_key_owners(&self, key: &K, replication_degree: usize) -> Vec<&NodeId> {
        key_owner_indices(key, self.cluster.len(), replication_degree)
            .into_iter()
            .map(|i| &self.cluster[i])
            .collect()
    }

    // convience function
    fn is_owner(&self, key: &K, replication_degree: usize) -> bool {
        self.get_key_owners(key, replication_degree)
            .iter()
            .any(|id| **id == self.my_node_id)
    }
}

// Pure function: given a sorted cluster and replication degree, return the
// indices of nodes that own this key.
// Separated for testing logic
fn key_owner_indices<K: Hash>(
    key: &K,
    cluster_len: usize,
    replication_degree: usize,
) -> Vec<usize> {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let start = (hasher.finish() as usize) % cluster_len;
    let degree = replication_degree.min(cluster_len);

    (0..degree).map(|i| (start + i) % cluster_len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cluster(n: usize) -> Vec<NodeId> {
        let names = [
            "ariel", "caliban", "callisto", "ceres", "chiron", "cupid", "eris", "europa", "hydra",
            "iapetus",
        ];
        (0..n)
            .map(|i| NodeId {
                sunlab_name: names[i].to_string(),
                id: i,
            })
            .collect()
    }

    #[test]
    fn single_owner_returns_one_node() {
        let cluster = make_cluster(5);
        let key: u64 = 42;
        let owners = key_owner_indices(&key, cluster.len(), 1);
        assert_eq!(owners.len(), 1);
        assert!(owners[0] < cluster.len());
    }

    #[test]
    fn replication_degree_returns_correct_count() {
        let cluster = make_cluster(5);
        let key: u64 = 42;

        for degree in 1..=5 {
            let owners = key_owner_indices(&key, cluster.len(), degree);
            assert_eq!(owners.len(), degree);
        }
    }

    #[test]
    fn no_duplicate_owners() {
        let cluster = make_cluster(5);
        let key: u64 = 99;
        let owners = key_owner_indices(&key, cluster.len(), 5);

        let mut unique = owners.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), owners.len());
    }

    #[test]
    fn owners_are_contiguous_on_ring() {
        let cluster = make_cluster(5);
        let key: u64 = 7;
        let owners = key_owner_indices(&key, cluster.len(), 3);

        // each subsequent owner should be (prev + 1) % cluster_len
        for i in 1..owners.len() {
            assert_eq!(owners[i], (owners[i - 1] + 1) % cluster.len());
        }
    }

    #[test]
    fn degree_capped_at_cluster_size() {
        let cluster = make_cluster(3);
        let key: u64 = 55;
        // request degree 10 but only 3 nodes exist
        let owners = key_owner_indices(&key, cluster.len(), 10);
        assert_eq!(owners.len(), 3);
    }

    #[test]
    fn same_key_same_owners() {
        let cluster = make_cluster(5);
        let key: u64 = 123;
        let a = key_owner_indices(&key, cluster.len(), 2);
        let b = key_owner_indices(&key, cluster.len(), 2);
        assert_eq!(a, b);
    }

    #[test]
    fn different_keys_distribute() {
        let cluster = make_cluster(5);
        let mut primary_counts = vec![0usize; cluster.len()];

        // hash a bunch of keys and count which node is primary
        for key in 0u64..1000 {
            let owners = key_owner_indices(&key, cluster.len(), 1);
            primary_counts[owners[0]] += 1;
        }

        // every node should get at least some keys (sanity check distribution)
        for (i, count) in primary_counts.iter().enumerate() {
            assert!(
                *count > 50,
                "node {} only got {} keys out of 1000, distribution looks broken",
                i,
                count
            );
        }
    }

    #[test]
    fn wraps_around_ring() {
        let cluster = make_cluster(5);
        // find a key whose primary is the last node
        let key = (0u64..10000)
            .find(|k| {
                let owners = key_owner_indices(k, cluster.len(), 1);
                owners[0] == cluster.len() - 1
            })
            .expect("should find a key mapping to last node");

        let owners = key_owner_indices(&key, cluster.len(), 3);
        assert_eq!(owners[0], cluster.len() - 1);
        assert_eq!(owners[1], 0); // wraps to first node
        assert_eq!(owners[2], 1);
    }
}
