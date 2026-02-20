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
    time::Duration,
};
use tokio::sync::OwnedMutexGuard;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error, info};

const COORDINATOR_VOTE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage<K, V>
where
    V: Clone,
    K: Clone,
{
    // asking a primary replica for a get
    Get {
        key: K,
        req_id: u64,
    },
    // primary replica responding to a get request
    GetResponse {
        val: Option<V>,
        req_id: u64,
    },
    // asking a primary replica for a put
    Put {
        pair: KVPair<K, V>,
        req_id: u64,
    },
    // primary replica responding to a put request
    PutResponse {
        success: bool,
        req_id: u64,
    },
    // message from a primary replica to process a put
    ReplicaPut {
        pair: KVPair<K, V>,
    },
    // 2PC messages:
    // Coordinator asking primary replicas to ready for a PUT
    Prepare {
        pairs: Vec<KVPair<K, V>>,
        tx_id: u64,
    },
    // Primary replica responding to coordinator saying they're ready to commit
    VotePrepared {
        tx_id: u64,
    },
    // Primary replica responding to coordinator saying they can't acquire locks
    VoteAbort {
        tx_id: u64,
    },
    // After coordinator has heard back from all nodes, broadcast to all primary replicas
    // Primary replicas either commit or abort the PUT
    Commit {
        tx_id: u64,
    },
    Abort {
        tx_id: u64,
    },
    // This node has finished its tests
    Done,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct KVPair<K, V>
where
    V: Clone,
    K: Clone,
{
    pub key: K,
    pub val: V,
}

#[derive(Debug)]
pub enum LocalMessage<K, V>
where
    V: Clone,
    K: Clone,
{
    Get {
        key: K,
        response_sender: oneshot::Sender<Option<V>>,
    },
    Put {
        pair: KVPair<K, V>,
        response_sender: oneshot::Sender<bool>,
    },
    TriPut {
        pairs: [KVPair<K, V>; 3],
        response_sender: oneshot::Sender<bool>,
    },
    // message from test harness when all tests finish
    Done,
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

struct PendingTx<K: Clone, V: Clone> {
    pairs: Vec<(KVPair<K, V>, usize)>, // (pair, stripe_index)
    guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>>,
}

pub struct Node<K, V>
where
    V: Clone,
    K: Clone,
{
    peers: Peers<K, V>,
    // all the nodes in this cluster, in consistent ordering
    cluster: Vec<NodeId>,
    // number of nodes that have finished their test
    done_count: AtomicUsize,
    // how many nodes hold each key
    replication_degree: usize,
    my_node_id: NodeId,
    local_inbox: mpsc::Receiver<LocalMessage<K, V>>,
    db: StripedDb<K, V>,
    awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>>,
    // Participant: holds locks between Prepare and Commit/Abort
    pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>>,
    // Coordinator: channels for collecting votes
    awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<bool>>>>,
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
                pending_prepares,
                awaiting_votes,
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
            // sends GET request to the primary replica
            LocalMessage::Get {
                key,
                response_sender,
            } => {
                let replicas = self.get_key_replicas(&key);

                // find the primary replica
                let primary_replica = replicas
                    .first()
                    .ok_or(anyhow!("Key {:?} doesn't have any replicas!", key))?;

                // if this replica is the primary, process the GET
                if **primary_replica == self.my_node_id {
                    let db = self.db.clone();
                    tokio::spawn(async move {
                        let resp = db.get(&key).await;
                        let _ = response_sender.send(resp);
                    });
                } else {
                    let req_id: u64 = rand::rng().random();
                    let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };
                    self.peers.send(primary_replica, request).await?;

                    // create a task awaiting the response from a peer
                    let (get_sender, get_receiver) = oneshot::channel();
                    {
                        let mut awaiting_get_response = self.awaiting_get_response.lock().await;
                        awaiting_get_response.insert(req_id, get_sender);
                    }

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
            }
            LocalMessage::Put {
                pair,
                response_sender,
            } => {
                let key = pair.key;
                let val = pair.val.clone();

                let replicas: Vec<NodeId> =
                    self.get_key_replicas(&key).into_iter().cloned().collect();

                // find the primary replica
                let primary_replica = replicas
                    .first()
                    .ok_or(anyhow!("Key {:?} doesn't have any replicas!", key))?
                    .clone();

                // if this node is the primary replica, execute the write and send the request to
                // your other replicas
                // Else, forward this request to the primary replica
                if primary_replica == self.my_node_id {
                    // make the write and broadcast to the other replicas.  Don't need to wait for
                    // a response because we assume no crashes
                    let db = self.db.clone();
                    let my_node_id = self.my_node_id.clone();
                    let all_senders = self.peers.senders.clone();
                    tokio::spawn(async move {
                        db.put(key, val).await;
                        // respond to test harness
                        let _ = response_sender.send(true);

                        let req = PeerMessage::ReplicaPut { pair };
                        for replica in replicas.iter().filter(|x| **x != my_node_id) {
                            if let Some(sender) = all_senders.get(replica) {
                                let _ = sender.send(req.clone()).await;
                            }
                        }
                    });
                } else {
                    // send this request to the primary replica and await the response
                    // TODO why do we have to await the response?  It only fails on crash
                    let req_id: u64 = rand::rng().random();
                    let req = PeerMessage::Put { pair, req_id };
                    self.peers.send(&primary_replica, req).await?;

                    // create a task awaiting the response from a peer
                    let (put_sender, put_receiver) = oneshot::channel();
                    {
                        let mut awaiting_put_response = self.awaiting_put_response.lock().await;
                        awaiting_put_response.insert(req_id, put_sender);
                    }

                    // start a non-blocking task that awaits the response from the primary and sends back to test
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
            // This node serves as the coordinator
            LocalMessage::TriPut {
                pairs,
                response_sender,
            } => {
                let tx_id: u64 = rand::rng().random();

                // Group pairs by primary, precompute stripe indices
                let mut primary_to_entries: HashMap<NodeId, Vec<(KVPair<K, V>, usize)>> =
                    HashMap::new();
                for pair in pairs {
                    let primary = self.get_key_replicas(&pair.key)[0].clone();
                    let stripe_idx = self.db.stripe_index(&pair.key);
                    primary_to_entries
                        .entry(primary)
                        .or_default()
                        .push((pair, stripe_idx));
                }

                let total_primaries = primary_to_entries.len();
                let (vote_tx, mut vote_rx) = mpsc::channel(total_primaries);

                // Prepare locally if this node is primary for any keys
                if let Some(mut local_entries) = primary_to_entries.remove(&self.my_node_id) {
                    let db = self.db.clone();
                    let pending_prepares = self.pending_prepares.clone();
                    let vote_tx_clone = vote_tx.clone();
                    tokio::spawn(async move {
                        // Sort by stripe index for deadlock prevention
                        local_entries.sort_by_key(|(_, idx)| *idx);

                        let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>> =
                            HashMap::new();
                        let mut lock_failed = false;
                        for (_, idx) in &local_entries {
                            if !guards.contains_key(idx) {
                                let stripe = db.get_stripe_by_index(*idx);
                                match stripe.try_lock_owned() {
                                    Ok(guard) => {
                                        guards.insert(*idx, guard);
                                    }
                                    Err(_) => {
                                        debug!("Local prepare try_lock failed for tx {}", tx_id);
                                        lock_failed = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if lock_failed {
                            // Vote abort — drop any acquired guards
                            let _ = vote_tx_clone.send(false).await;
                        } else {
                            pending_prepares.lock().await.insert(
                                tx_id,
                                PendingTx {
                                    pairs: local_entries,
                                    guards,
                                },
                            );

                            // Local prepare succeeds
                            let _ = vote_tx_clone.send(true).await;
                        }
                    });
                }

                // Send Prepare to remote primaries
                let remote_primaries: Vec<NodeId> = primary_to_entries.keys().cloned().collect();
                for (primary, entries) in &primary_to_entries {
                    let pairs_only: Vec<KVPair<K, V>> =
                        entries.iter().map(|(p, _)| p.clone()).collect();
                    let msg = PeerMessage::Prepare {
                        pairs: pairs_only,
                        tx_id,
                    };
                    self.peers.send(primary, msg).await?;
                }

                // Register vote channel for remote votes
                if !remote_primaries.is_empty() {
                    self.awaiting_votes.lock().await.insert(tx_id, vote_tx);
                }

                // Clone everything the spawned task needs
                let pending_prepares = self.pending_prepares.clone();
                let awaiting_votes = self.awaiting_votes.clone();
                let my_node_id = self.my_node_id.clone();
                let cluster = self.cluster.clone();
                let replication_degree = self.replication_degree;
                let all_senders = self.peers.senders.clone();

                // Spawn coordinator task to collect votes and commit/abort
                tokio::spawn(async move {
                    // Collect votes with a timeout to prevent distributed deadlock
                    let collect_result = tokio::time::timeout(COORDINATOR_VOTE_TIMEOUT, async {
                        for _ in 0..total_primaries {
                            match vote_rx.recv().await {
                                Some(true) => {}
                                _ => return false,
                            }
                        }
                        true
                    })
                    .await;

                    let all_prepared = matches!(collect_result, Ok(true));

                    awaiting_votes.lock().await.remove(&tx_id);

                    if all_prepared {
                        // Commit locally: write values and replicate
                        {
                            let mut pending = pending_prepares.lock().await;
                            if let Some(mut tx) = pending.remove(&tx_id) {
                                // Write into held guards
                                for (pair, stripe_idx) in &tx.pairs {
                                    if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                                        guard.insert(pair.key, pair.val.clone());
                                    }
                                }

                                // Collect replication info before dropping guards
                                let to_replicate: Vec<KVPair<K, V>> =
                                    tx.pairs.iter().map(|(p, _)| p.clone()).collect();

                                // Drop guards to release stripe locks
                                drop(tx);
                                drop(pending);

                                // Send ReplicaPut for locally-owned pairs
                                for pair in to_replicate {
                                    let replicas = key_replica_indices(
                                        &pair.key,
                                        cluster.len(),
                                        replication_degree,
                                    );
                                    for r_idx in replicas {
                                        let replica = &cluster[r_idx];
                                        if *replica != my_node_id {
                                            if let Some(sender) = all_senders.get(replica) {
                                                let msg =
                                                    PeerMessage::ReplicaPut { pair: pair.clone() };
                                                let _ = sender.send(msg).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Send Commit to remote primaries
                        for primary in &remote_primaries {
                            if let Some(sender) = all_senders.get(primary) {
                                let msg = PeerMessage::Commit { tx_id };
                                let _ = sender.send(msg).await;
                            }
                        }

                        let _ = response_sender.send(true);
                    } else {
                        // Abort locally: just drop guards
                        pending_prepares.lock().await.remove(&tx_id);

                        // Send Abort to remote primaries
                        for primary in &remote_primaries {
                            if let Some(sender) = all_senders.get(primary) {
                                let msg = PeerMessage::Abort { tx_id };
                                let _ = sender.send(msg).await;
                            }
                        }

                        let _ = response_sender.send(false);
                    }
                });
            }
            // tell other peers that I'm done with my tests
            LocalMessage::Done => {
                let my_node_id = self.my_node_id.clone();
                for (node_id, sender) in self.peers.senders.iter() {
                    if *node_id != my_node_id {
                        let _ = sender.send(PeerMessage::Done).await;
                    }
                }
            }
        }

        Ok(())
    }

    // handles messages from the network
    async fn handle_peer_message(&self, from: NodeId, msg: PeerMessage<K, V>) -> Result<bool> {
        debug!("Got {:?} from {}", msg, from);
        match msg {
            // peer is asking us for a get request because we are the primary replica
            PeerMessage::Get { key, req_id } => {
                let db = self.db.clone();
                let senders = self.peers.senders.clone();
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
            // peer is asking us for a put request because we are the primary replica
            PeerMessage::Put { pair, req_id } => {
                let db = self.db.clone();
                let my_node_id = self.my_node_id.clone();
                let cluster = self.cluster.clone();
                let replication_degree = self.replication_degree;
                let senders = self.peers.senders.clone();
                tokio::spawn(async move {
                    let key = pair.key;
                    let val = pair.val.clone();
                    let result = db.put(key, val).await;
                    let resp: PeerMessage<K, V> = PeerMessage::PutResponse {
                        success: result,
                        req_id,
                    };

                    // broadcast this put to other replicas
                    let replicas = key_replica_indices(&key, cluster.len(), replication_degree);
                    let req = PeerMessage::ReplicaPut { pair };
                    for r_idx in &replicas {
                        let replica = &cluster[*r_idx];
                        if *replica != my_node_id {
                            if let Some(sender) = senders.get(replica) {
                                let _ = sender.send(req.clone()).await;
                            }
                        }
                    }

                    // respond to original peer that the put was successful
                    if let Some(sender) = senders.get(&from) {
                        let _ = sender.send(resp).await;
                    }
                });
            }
            // the primary replica is telling us to execute this put
            PeerMessage::ReplicaPut { pair } => {
                let db = self.db.clone();
                tokio::spawn(async move {
                    db.put(pair.key, pair.val).await;
                });
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
            PeerMessage::Abort { tx_id } => {
                // Just drop the guards, releasing locks
                self.pending_prepares.lock().await.remove(&tx_id);
            }
            // primary replica receives a prepare message from coordinator
            PeerMessage::Prepare { pairs, tx_id } => {
                let db = self.db.clone();
                let pending_prepares = self.pending_prepares.clone();
                let senders = self.peers.senders.clone();
                tokio::spawn(async move {
                    // Compute stripe indices and sort for deadlock prevention
                    let mut entries: Vec<(KVPair<K, V>, usize)> = pairs
                        .iter()
                        .map(|p| (p.clone(), db.stripe_index(&p.key)))
                        .collect();
                    entries.sort_by_key(|(_, idx)| *idx);

                    // Try to acquire stripe locks in sorted order
                    let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>> = HashMap::new();
                    let mut lock_failed = false;
                    for (_, idx) in &entries {
                        if !guards.contains_key(idx) {
                            let stripe = db.get_stripe_by_index(*idx);
                            match stripe.try_lock_owned() {
                                Ok(guard) => {
                                    guards.insert(*idx, guard);
                                }
                                Err(_) => {
                                    debug!(
                                        "Prepare try_lock failed for tx {} on stripe {}",
                                        tx_id, idx
                                    );
                                    lock_failed = true;
                                    break;
                                }
                            }
                        }
                    }

                    if lock_failed {
                        // Vote abort — drop any acquired guards
                        if let Some(sender) = senders.get(&from) {
                            let resp = PeerMessage::VoteAbort { tx_id };
                            let _ = sender.send(resp).await;
                        }
                    } else {
                        // Store pending transaction
                        pending_prepares.lock().await.insert(
                            tx_id,
                            PendingTx {
                                pairs: entries,
                                guards,
                            },
                        );

                        // Vote prepared
                        if let Some(sender) = senders.get(&from) {
                            let resp = PeerMessage::VotePrepared { tx_id };
                            let _ = sender.send(resp).await;
                        }
                    }
                });
            }
            // Coordinator receives a prepare vote from a primary replica
            PeerMessage::VotePrepared { tx_id } => {
                let awaiting = self.awaiting_votes.lock().await;
                if let Some(tx) = awaiting.get(&tx_id) {
                    let _ = tx.send(true).await;
                }
            }
            // Coordinator receives an abort vote from a primary replica
            PeerMessage::VoteAbort { tx_id } => {
                let awaiting = self.awaiting_votes.lock().await;
                if let Some(tx) = awaiting.get(&tx_id) {
                    let _ = tx.send(false).await;
                }
            }
            // primary replica can commit a txn
            PeerMessage::Commit { tx_id } => {
                let pending_prepares = self.pending_prepares.clone();
                let my_node_id = self.my_node_id.clone();
                let cluster = self.cluster.clone();
                let replication_degree = self.replication_degree;
                let senders = self.peers.senders.clone();
                tokio::spawn(async move {
                    let mut pending = pending_prepares.lock().await;
                    if let Some(mut tx) = pending.remove(&tx_id) {
                        // Write values into held guards
                        for (pair, stripe_idx) in &tx.pairs {
                            if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                                guard.insert(pair.key, pair.val.clone());
                            }
                        }

                        // Collect replication info before dropping
                        let to_replicate: Vec<KVPair<K, V>> =
                            tx.pairs.iter().map(|(p, _)| p.clone()).collect();

                        // Release locks
                        drop(tx);
                        drop(pending);

                        // Replicate to non-primary replicas
                        for pair in to_replicate {
                            let replicas =
                                key_replica_indices(&pair.key, cluster.len(), replication_degree);
                            for r_idx in replicas {
                                let replica = &cluster[r_idx];
                                if *replica != my_node_id {
                                    if let Some(sender) = senders.get(replica) {
                                        let msg = PeerMessage::ReplicaPut { pair: pair.clone() };
                                        let _ = sender.send(msg).await;
                                    }
                                }
                            }
                        }
                    }
                });
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

    // maps the key to the replica nodes
    fn get_key_replicas(&self, key: &K) -> Vec<&NodeId> {
        key_replica_indices(key, self.cluster.len(), self.replication_degree)
            .into_iter()
            .map(|i| &self.cluster[i])
            .collect()
    }
}

// Pure function: given a sorted cluster and replication degree, return the
// indices of nodes that own this key.
// Separated for testing logic
fn key_replica_indices<K: Hash>(
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

        // each subsequent replica should be (prev + 1) % cluster_len
        for i in 1..replicas.len() {
            assert_eq!(replicas[i], (replicas[i - 1] + 1) % cluster.len());
        }
    }

    #[test]
    fn degree_capped_at_cluster_size() {
        let cluster = make_cluster(3);
        let key: u64 = 55;
        // request degree 10 but only 3 nodes exist
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

        // hash a bunch of keys and count which node is primary
        for key in 0u64..1000 {
            let replicas = key_replica_indices(&key, cluster.len(), 1);
            primary_counts[replicas[0]] += 1;
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
                let replicas = key_replica_indices(k, cluster.len(), 1);
                replicas[0] == cluster.len() - 1
            })
            .expect("should find a key mapping to last node");

        let replicas = key_replica_indices(&key, cluster.len(), 3);
        assert_eq!(replicas[0], cluster.len() - 1);
        assert_eq!(replicas[1], 0); // wraps to first node
        assert_eq!(replicas[2], 1);
    }
}
