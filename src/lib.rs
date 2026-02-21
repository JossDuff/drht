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
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tracing::{debug, error, info};

const CHANNEL_BUFFER_SIZE: usize = 4;
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

// Shared state accessible by both tasks: local message handler and peer message handler
struct Shared<K: Clone, V: Clone> {
    senders: HashMap<NodeId, mpsc::Sender<PeerMessage<K, V>>>,
    cluster: Vec<NodeId>,
    my_node_id: NodeId,
    replication_degree: usize,
    db: StripedDb<K, V>,
    awaiting_put_response: Arc<Mutex<HashMap<u64, oneshot::Sender<bool>>>>,
    awaiting_get_response: Arc<Mutex<HashMap<u64, oneshot::Sender<Option<V>>>>>,
    pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>>,
    awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<bool>>>>,
    done_count: AtomicUsize,
    // signaled when done_count reaches cluster.len()
    shutdown: Notify,
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
    fn mark_done(&self) {
        let count = self.done_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.cluster.len() {
            self.shutdown.notify_waiters();
        }
    }

    fn get_key_replicas(&self, key: &K) -> Vec<&NodeId> {
        key_replica_indices(key, self.cluster.len(), self.replication_degree)
            .into_iter()
            .map(|i| &self.cluster[i])
            .collect()
    }

    // Send a message to a peer.  Awaits until the bounded channel has space.
    // This is fine because each task has its own receiver — a blocked send
    // here can never prevent the *other* task from draining its inbox.
    async fn send_to_peer(&self, target: &NodeId, msg: PeerMessage<K, V>) -> Result<()> {
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
    //
    // The local task can freely `.await` on sends to peers without blocking
    // the peer task from draining its inbox
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
                let replicas = s.get_key_replicas(&key);
                let primary_replica = replicas
                    .first()
                    .ok_or(anyhow!("Key {:?} doesn't have any replicas!", key))?
                    .clone()
                    .clone();

                if primary_replica == s.my_node_id {
                    let db = s.db.clone();
                    tokio::spawn(async move {
                        let resp = db.get(&key).await;
                        let _ = response_sender.send(resp);
                    });
                } else {
                    let req_id: u64 = rand::rng().random();
                    let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };

                    // Register the response channel BEFORE sending so there's
                    // no race with the reply arriving first.
                    let (get_sender, get_receiver) = oneshot::channel();
                    {
                        let mut awaiting = s.awaiting_get_response.lock().await;
                        awaiting.insert(req_id, get_sender);
                    }

                    // This .await may block if the peer's channel is full.
                    s.send_to_peer(&primary_replica, request).await?;

                    tokio::spawn(async move {
                        match get_receiver.await {
                            Ok(peer_get_response) => {
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

                let replicas: Vec<NodeId> = s.get_key_replicas(&key).into_iter().cloned().collect();
                let primary_replica = replicas
                    .first()
                    .ok_or(anyhow!("Key {:?} doesn't have any replicas!", key))?
                    .clone();

                if primary_replica == s.my_node_id {
                    let db = s.db.clone();
                    let my_node_id = s.my_node_id.clone();
                    let senders = s.senders.clone();
                    tokio::spawn(async move {
                        db.put(key, val).await;
                        let _ = response_sender.send(true);

                        let req = PeerMessage::ReplicaPut { pair };
                        for replica in replicas.iter().filter(|x| **x != my_node_id) {
                            if let Some(sender) = senders.get(replica) {
                                let _ = sender.send(req.clone()).await;
                            }
                        }
                    });
                } else {
                    let req_id: u64 = rand::rng().random();
                    let req = PeerMessage::Put { pair, req_id };

                    // Register BEFORE sending
                    let (put_sender, put_receiver) = oneshot::channel();
                    {
                        let mut awaiting = s.awaiting_put_response.lock().await;
                        awaiting.insert(req_id, put_sender);
                    }

                    s.send_to_peer(&primary_replica, req).await?;

                    tokio::spawn(async move {
                        match put_receiver.await {
                            Ok(peer_put_response) => {
                                let _ = response_sender.send(peer_put_response);
                            }
                            Err(e) => {
                                error!("Receive error waiting for putResponse: {}", e);
                            }
                        }
                    });
                }
            }
            // This node is the coordinator for the TRIPUT
            LocalMessage::TriPut {
                pairs,
                response_sender,
            } => {
                let tx_id: u64 = rand::rng().random();

                // Group pairs by primary, precompute stripe indices
                let mut primary_to_entries: HashMap<NodeId, Vec<(KVPair<K, V>, usize)>> =
                    HashMap::new();
                for pair in pairs {
                    let primary = s.get_key_replicas(&pair.key)[0].clone();
                    let stripe_idx = s.db.stripe_index(&pair.key);
                    primary_to_entries
                        .entry(primary)
                        .or_default()
                        .push((pair, stripe_idx));
                }

                let total_primaries = primary_to_entries.len();
                let (vote_tx, mut vote_rx) = mpsc::channel(total_primaries);

                // Prepare locally if this node is primary for any keys
                if let Some(mut local_entries) = primary_to_entries.remove(&s.my_node_id) {
                    let db = s.db.clone();
                    let pending_prepares = s.pending_prepares.clone();
                    let vote_tx_clone = vote_tx.clone();
                    tokio::spawn(async move {
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
                            let _ = vote_tx_clone.send(false).await;
                        } else {
                            pending_prepares.lock().await.insert(
                                tx_id,
                                PendingTx {
                                    pairs: local_entries,
                                    guards,
                                },
                            );
                            let _ = vote_tx_clone.send(true).await;
                        }
                    });
                }

                // Send Prepare to remote primaries — safe to block here
                let remote_primaries: Vec<NodeId> = primary_to_entries.keys().cloned().collect();
                for (primary, entries) in &primary_to_entries {
                    let pairs_only: Vec<KVPair<K, V>> =
                        entries.iter().map(|(p, _)| p.clone()).collect();
                    let msg = PeerMessage::Prepare {
                        pairs: pairs_only,
                        tx_id,
                    };
                    s.send_to_peer(primary, msg).await?;
                }

                // Register vote channel for remote votes
                if !remote_primaries.is_empty() {
                    s.awaiting_votes.lock().await.insert(tx_id, vote_tx);
                }

                // Clone everything the spawned coordinator task needs
                let pending_prepares = s.pending_prepares.clone();
                let awaiting_votes = s.awaiting_votes.clone();
                let my_node_id = s.my_node_id.clone();
                let cluster = s.cluster.clone();
                let replication_degree = s.replication_degree;
                let all_senders = s.senders.clone();

                tokio::spawn(async move {
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
                        {
                            let mut pending = pending_prepares.lock().await;
                            if let Some(mut tx) = pending.remove(&tx_id) {
                                for (pair, stripe_idx) in &tx.pairs {
                                    if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                                        guard.insert(pair.key, pair.val.clone());
                                    }
                                }

                                let to_replicate: Vec<KVPair<K, V>> =
                                    tx.pairs.iter().map(|(p, _)| p.clone()).collect();

                                drop(tx);
                                drop(pending);

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

                        for primary in &remote_primaries {
                            if let Some(sender) = all_senders.get(primary) {
                                let msg = PeerMessage::Commit { tx_id };
                                let _ = sender.send(msg).await;
                            }
                        }

                        let _ = response_sender.send(true);
                    } else {
                        pending_prepares.lock().await.remove(&tx_id);

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
            // ── peer asks us for a GET (we're primary) ───────────────
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

            // ── peer asks us for a PUT (we're primary) ───────────────
            PeerMessage::Put { pair, req_id } => {
                let db = s.db.clone();
                let my_node_id = s.my_node_id.clone();
                let cluster = s.cluster.clone();
                let replication_degree = s.replication_degree;
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    let key = pair.key;
                    let val = pair.val.clone();
                    let result = db.put(key, val).await;
                    let resp: PeerMessage<K, V> = PeerMessage::PutResponse {
                        success: result,
                        req_id,
                    };

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

                    if let Some(sender) = senders.get(&from) {
                        let _ = sender.send(resp).await;
                    }
                });
            }

            // ── replica put from primary ─────────────────────────────
            PeerMessage::ReplicaPut { pair } => {
                let db = s.db.clone();
                tokio::spawn(async move {
                    db.put(pair.key, pair.val).await;
                });
            }

            // ── response to our earlier GET request ──────────────────
            PeerMessage::GetResponse { val, req_id } => {
                let mut awaiting = s.awaiting_get_response.lock().await;
                match awaiting.remove(&req_id) {
                    Some(sender) => {
                        sender.send(val).map_err(|_| {
                            anyhow!("Error sending local GetResponse for request {}", req_id)
                        })?;
                    }
                    None => {
                        return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                    }
                };
            }

            // ── response to our earlier PUT request ──────────────────
            PeerMessage::PutResponse { success, req_id } => {
                let mut awaiting = s.awaiting_put_response.lock().await;
                match awaiting.remove(&req_id) {
                    Some(sender) => {
                        sender.send(success).map_err(|_| {
                            anyhow!("Error sending local PutResponse for request {}", req_id)
                        })?;
                    }
                    None => {
                        return Err(anyhow!("Receiver for req_id {} dropped", req_id));
                    }
                };
            }

            // ── 2PC: abort from coordinator ──────────────────────────
            PeerMessage::Abort { tx_id } => {
                s.pending_prepares.lock().await.remove(&tx_id);
            }

            // ── 2PC: prepare from coordinator (we're a participant) ──
            PeerMessage::Prepare { pairs, tx_id } => {
                let db = s.db.clone();
                let pending_prepares = s.pending_prepares.clone();
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    let mut entries: Vec<(KVPair<K, V>, usize)> = pairs
                        .iter()
                        .map(|p| (p.clone(), db.stripe_index(&p.key)))
                        .collect();
                    entries.sort_by_key(|(_, idx)| *idx);

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
                        if let Some(sender) = senders.get(&from) {
                            let resp = PeerMessage::VoteAbort { tx_id };
                            let _ = sender.send(resp).await;
                        }
                    } else {
                        pending_prepares.lock().await.insert(
                            tx_id,
                            PendingTx {
                                pairs: entries,
                                guards,
                            },
                        );

                        if let Some(sender) = senders.get(&from) {
                            let resp = PeerMessage::VotePrepared { tx_id };
                            let _ = sender.send(resp).await;
                        }
                    }
                });
            }

            // ── 2PC: vote responses (we're coordinator) ──────────────
            PeerMessage::VotePrepared { tx_id } => {
                let awaiting = s.awaiting_votes.lock().await;
                if let Some(tx) = awaiting.get(&tx_id) {
                    let _ = tx.send(true).await;
                }
            }
            PeerMessage::VoteAbort { tx_id } => {
                let awaiting = s.awaiting_votes.lock().await;
                if let Some(tx) = awaiting.get(&tx_id) {
                    let _ = tx.send(false).await;
                }
            }

            // ── 2PC: commit from coordinator ─────────────────────────
            PeerMessage::Commit { tx_id } => {
                let pending_prepares = s.pending_prepares.clone();
                let my_node_id = s.my_node_id.clone();
                let cluster = s.cluster.clone();
                let replication_degree = s.replication_degree;
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    let mut pending = pending_prepares.lock().await;
                    if let Some(mut tx) = pending.remove(&tx_id) {
                        for (pair, stripe_idx) in &tx.pairs {
                            if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                                guard.insert(pair.key, pair.val.clone());
                            }
                        }

                        let to_replicate: Vec<KVPair<K, V>> =
                            tx.pairs.iter().map(|(p, _)| p.clone()).collect();

                        drop(tx);
                        drop(pending);

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

            // ── peer finished their test ─────────────────────────────
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
