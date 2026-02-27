mod config;
mod db;
mod handlers;
mod messages;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use db::StripedDb;
use handlers::{
    handle_local_get, handle_local_put, handle_local_triput, handle_peer_commit,
    handle_peer_prepare, handle_peer_put,
};
pub use messages::{LocalMessage, PeerMessage};
use net::{connect_all, Peers};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::OwnedMutexGuard;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tracing::{debug, error, info, warn};

const CHANNEL_BUFFER_SIZE: usize = 64;
pub(crate) const COORDINATOR_VOTE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct KVPair<K, V>
where
    V: Clone,
    K: Clone,
{
    pub key: K,
    pub val: V,
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

pub(crate) struct PendingTx<K: Clone, V: Clone> {
    pub(crate) pairs: Vec<(KVPair<K, V>, usize)>, // (pair, stripe_index)
    pub(crate) guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>>,
}

// Shared state accessible by both tasks: local message handler and peer message handler
pub(crate) struct Shared<K: Clone, V: Clone> {
    pub(crate) senders: HashMap<NodeId, mpsc::Sender<PeerMessage<K, V>>>,
    pub(crate) cluster: Vec<NodeId>,
    pub(crate) my_node_id: NodeId,
    pub(crate) replication_degree: usize,
    pub(crate) db: StripedDb<K, V>,
    pub(crate) awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    pub(crate) awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>>,
    pub(crate) pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>>,
    pub(crate) awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<bool>>>>,
    pub(crate) done_count: AtomicUsize,
    // signaled when done_count reaches cluster.len()
    pub(crate) shutdown: Notify,
}

impl<K, V> Shared<K, V>
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
    // Increment done_count and signal shutdown if all nodes are done.
    pub(crate) fn mark_done(&self) {
        let count = self.done_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.cluster.len() {
            self.shutdown.notify_waiters();
        }
    }

    pub(crate) fn get_key_replicas(&self, key: &K) -> Vec<&NodeId> {
        key_replica_indices(key, self.cluster.len(), self.replication_degree)
            .into_iter()
            .map(|i| &self.cluster[i])
            .collect()
    }

    // Send a message to a peer.  Awaits until the bounded channel has space.
    // This is fine because each task has its own receiver — a blocked send
    // here can never prevent the *other* task from draining its inbox.
    pub(crate) async fn send_to_peer(&self, target: &NodeId, msg: PeerMessage<K, V>) -> Result<()> {
        if let Some(sender) = self.senders.get(target) {
            sender
                .send(msg)
                .await
                .map_err(|_| anyhow!("Channel to {} closed", target))?;
        }
        Ok(())
    }
}

pub struct Node<K, V>
where
    V: Clone,
    K: Clone,
{
    shared: Arc<Shared<K, V>>,
    local_inbox: mpsc::Receiver<LocalMessage<K, V>>,
    peer_inbox: mpsc::Receiver<(NodeId, PeerMessage<K, V>)>,
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
    pub async fn new(
        config: Config,
        net_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, mpsc::Sender<LocalMessage<K, V>>)> {
        let db = StripedDb::new(config.stripes);
        let awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<bool>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // this is a barrier
        let (peers, cluster, my_node_id): (Peers<K, V>, Vec<NodeId>, NodeId) =
            connect_all::<K, V>(&config.name, &config.connections, net_handle).await?;

        // for sending/ receiving messages from the test harness
        let (local_sender, local_inbox) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        // Destructure Peers so we can give the inbox to one task and
        // the senders map to the shared state.
        let peer_inbox = peers.inbox;
        let senders = peers.senders;

        let shared = Arc::new(Shared {
            senders,
            cluster,
            my_node_id,
            replication_degree: config.repication_degree,
            db,
            awaiting_get_response,
            awaiting_put_response,
            pending_prepares,
            awaiting_votes,
            done_count: AtomicUsize::new(0),
            shutdown: Notify::new(),
        });

        Ok((
            Self {
                shared,
                local_inbox,
                peer_inbox,
            },
            local_sender,
        ))
    }

    // Split into two independent tasks, each with its own receiver.
    pub async fn run(self) -> Result<()> {
        let shared_local = self.shared.clone();
        let shared_peer = self.shared.clone();
        let shutdown = self.shared.clone();

        let local_handle = tokio::spawn(run_local_loop(shared_local, self.local_inbox));
        let peer_handle = tokio::spawn(run_peer_loop(shared_peer, self.peer_inbox));

        // Wait for shutdown signal (done_count >= cluster.len())
        shutdown.shutdown.notified().await;
        info!("All peers done, shutting down");

        // Cancel both loops.  We don't strictly need to abort
        local_handle.abort();
        peer_handle.abort();

        // Swallow JoinErrors from the aborts
        let _ = local_handle.await;
        let _ = peer_handle.await;

        Ok(())
    }
}

async fn run_local_loop<K, V>(
    s: Arc<Shared<K, V>>,
    mut inbox: mpsc::Receiver<LocalMessage<K, V>>,
) -> Result<()>
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
    while let Some(msg) = inbox.recv().await {
        debug!("Got {:?}", msg);
        match msg {
            LocalMessage::Get {
                key,
                response_sender,
            } => {
                handle_local_get(&s, key, response_sender).await?;
            }
            LocalMessage::Put {
                pair,
                response_sender,
            } => {
                handle_local_put(&s, pair, response_sender).await?;
            }
            // This node is the coordinator for the TRIPUT
            LocalMessage::TriPut {
                pairs,
                response_sender,
            } => {
                handle_local_triput(&s, pairs, response_sender).await;
            }
            LocalMessage::Done => {
                info!("I am done with my tests, notifying peers");
                let my_node_id = s.my_node_id.clone();
                for (node_id, sender) in s.senders.iter() {
                    if *node_id != my_node_id {
                        let _ = sender.send(PeerMessage::Done).await;
                    }
                }
                s.mark_done();
            }
        }
    }

    Ok(())
}

async fn run_peer_loop<K, V>(
    s: Arc<Shared<K, V>>,
    mut inbox: mpsc::Receiver<(NodeId, PeerMessage<K, V>)>,
) -> Result<()>
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
    while let Some((from, msg)) = inbox.recv().await {
        debug!("Got {:?} from {}", msg, from);
        match msg {
            // peer asks us for a GET (we're primary)
            PeerMessage::Get { key, req_id } => {
                let db = s.db.clone();
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    let result = db.get(&key).await;
                    let resp: PeerMessage<K, V> = PeerMessage::GetResponse {
                        val: result,
                        req_id,
                    };
                    if let Some(sender) = senders.get(&from) {
                        let _ = sender.send(resp).await;
                    }
                });
            }

            // peer asks us for a PUT (we're primary)
            PeerMessage::Put { pair, req_id } => {
                handle_peer_put(&s, from, pair, req_id);
            }

            // replica put from primary
            PeerMessage::ReplicaPut { pair } => {
                let db = s.db.clone();
                tokio::spawn(async move {
                    db.put(pair.key, pair.val).await;
                });
            }

            // response to our earlier GET request
            PeerMessage::GetResponse { val, req_id } => {
                let mut awaiting = s.awaiting_get_response.lock().await;
                match awaiting.remove(&req_id) {
                    Some(sender) => {
                        if sender.send(val).is_err() {
                            error!("Failed to deliver GetResponse for req {}", req_id);
                        }
                    }
                    None => {
                        error!("GetResponse for unknown req_id {}", req_id);
                    }
                };
            }

            // response to our earlier PUT request
            PeerMessage::PutResponse { success, req_id } => {
                let mut awaiting = s.awaiting_put_response.lock().await;
                match awaiting.remove(&req_id) {
                    Some(sender) => {
                        if sender.send(success).is_err() {
                            error!("Failed to deliver PutResponse for req {}", req_id);
                        }
                    }
                    None => {
                        error!("PutResponse for unknown req_id {}", req_id);
                    }
                };
            }

            // 2PC: abort from coordinator
            PeerMessage::Abort { tx_id } => {
                debug!("2pc abort: Received Abort from coordinator for tx_id {tx_id}, removing from pending_prepares...");
                s.pending_prepares.lock().await.remove(&tx_id);
                debug!("2pc abort: Successfully removed {tx_id} from pending_prepares");
            }

            // 2PC: prepare from coordinator
            PeerMessage::Prepare { pairs, tx_id } => {
                handle_peer_prepare(&s, from, pairs, tx_id);
            }

            // 2PC: vote responses (we're coordinator)
            PeerMessage::VotePrepared { tx_id } => {
                let tx = {
                    debug!("2pc voteprepated: getting lock on awaiting_votes for {tx_id}...");
                    let awaiting = s.awaiting_votes.lock().await;
                    debug!("2pc voteprepated: got lock on awaiting_votes for {tx_id}");
                    awaiting.get(&tx_id).cloned()
                };
                if let Some(tx) = tx {
                    let _ = tx.send(true).await;
                } else {
                    warn!("2pc voteprepared: No sender for {tx_id}");
                }
            }
            PeerMessage::VoteAbort { tx_id } => {
                let tx = {
                    debug!("2pc voteabort: getting lock on awaiting_votes for {tx_id}...");
                    let awaiting = s.awaiting_votes.lock().await;
                    debug!("2pc voteabort: got lock on awaiting_votes for {tx_id}");
                    awaiting.get(&tx_id).cloned()
                };
                if let Some(tx) = tx {
                    let _ = tx.send(false).await;
                } else {
                    warn!("2pc voteabort: No sender for {tx_id}");
                }
            }

            // 2PC: commit from coordinator
            PeerMessage::Commit { tx_id } => {
                handle_peer_commit(&s, tx_id);
            }

            // peer finished their test
            PeerMessage::Done => {
                info!("{} is done with their test", from);
                s.mark_done();
            }
        }
    }

    Ok(())
}

// Pure function: given a sorted cluster and replication degree, return the
// indices of nodes that own this key.
pub(crate) fn key_replica_indices<K: Hash>(
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
    fn single_replica_returns_one_node() {
        let cluster = make_cluster(5);
        let key: u64 = 42;
        let replicas = key_replica_indices(&key, cluster.len(), 1);
        assert_eq!(replicas.len(), 1);
        assert!(replicas[0] < cluster.len());
    }

    #[test]
    fn replication_degree_returns_correct_count() {
        let cluster = make_cluster(5);
        let key: u64 = 42;

        for degree in 1..=5 {
            let replicas = key_replica_indices(&key, cluster.len(), degree);
            assert_eq!(replicas.len(), degree);
        }
    }

    #[test]
    fn no_duplicate_replicas() {
        let cluster = make_cluster(5);
        let key: u64 = 99;
        let replicas = key_replica_indices(&key, cluster.len(), 5);

        let mut unique = replicas.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), replicas.len());
    }

    #[test]
    fn replicas_are_contiguous_on_ring() {
        let cluster = make_cluster(5);
        let key: u64 = 7;
        let replicas = key_replica_indices(&key, cluster.len(), 3);

        for i in 1..replicas.len() {
            assert_eq!(replicas[i], (replicas[i - 1] + 1) % cluster.len());
        }
    }

    #[test]
    fn degree_capped_at_cluster_size() {
        let cluster = make_cluster(3);
        let key: u64 = 55;
        let replicas = key_replica_indices(&key, cluster.len(), 10);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn same_key_same_replicas() {
        let cluster = make_cluster(5);
        let key: u64 = 123;
        let a = key_replica_indices(&key, cluster.len(), 2);
        let b = key_replica_indices(&key, cluster.len(), 2);
        assert_eq!(a, b);
    }

    #[test]
    fn different_keys_distribute() {
        let cluster = make_cluster(5);
        let mut primary_counts = vec![0usize; cluster.len()];

        for key in 0u64..1000 {
            let replicas = key_replica_indices(&key, cluster.len(), 1);
            primary_counts[replicas[0]] += 1;
        }

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
        let key = (0u64..10000)
            .find(|k| {
                let replicas = key_replica_indices(k, cluster.len(), 1);
                replicas[0] == cluster.len() - 1
            })
            .expect("should find a key mapping to last node");

        let replicas = key_replica_indices(&key, cluster.len(), 3);
        assert_eq!(replicas[0], cluster.len() - 1);
        assert_eq!(replicas[1], 0);
        assert_eq!(replicas[2], 1);
    }
}
