mod config;
mod db;
mod messages;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use db::StripedDb;
pub use messages::{LocalMessage, PeerMessage};
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
use tracing::{debug, error, info, warn};

const CHANNEL_BUFFER_SIZE: usize = 64;
const COORDINATOR_VOTE_TIMEOUT: Duration = Duration::from_millis(200);

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
                let replicas = s.get_key_replicas(&key);
                let primary_replica = replicas
                    .first()
                    .ok_or(anyhow!("Key {:?} doesn't have any replicas!", key))?;

                if **primary_replica == s.my_node_id {
                    let db = s.db.clone();
                    tokio::spawn(async move {
                        let resp = db.get(&key).await;
                        let _ = response_sender.send(resp);
                    });
                } else {
                    let req_id: u64 = rand::rng().random();
                    let request: PeerMessage<K, V> = PeerMessage::Get { key, req_id };

                    // register reciever
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

                    // register response receiver
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

                // Extract local entries (if any) — handled inline in
                // the coordinator task to avoid a race where the
                // coordinator aborts before a separately-spawned local
                // prepare inserts its guards, orphaning stripe locks.
                let local_entries = primary_to_entries.remove(&s.my_node_id);

                // Compute remote primaries and build prepare messages
                let remote_primaries: Vec<NodeId> = primary_to_entries.keys().cloned().collect();
                let remote_prepare_msgs: Vec<(NodeId, PeerMessage<K, V>)> = primary_to_entries
                    .iter()
                    .map(|(primary, entries)| {
                        let pairs_only: Vec<KVPair<K, V>> =
                            entries.iter().map(|(p, _)| p.clone()).collect();
                        (
                            primary.clone(),
                            PeerMessage::Prepare {
                                pairs: pairs_only,
                                tx_id,
                            },
                        )
                    })
                    .collect();

                // Vote channel only carries remote votes; local result
                // is known synchronously inside the coordinator task.
                let num_remote = remote_primaries.len();
                let (vote_tx, mut vote_rx) = mpsc::channel(num_remote.max(1));

                // register response channel
                if !remote_primaries.is_empty() {
                    debug!("Getting lock on awaiting_votes for {tx_id}");
                    s.awaiting_votes.lock().await.insert(tx_id, vote_tx);
                    debug!("Added {} to awaiting_votes", tx_id);
                }

                // Clone everything the spawned coordinator task needs
                let db = s.db.clone();
                let pending_prepares = s.pending_prepares.clone();
                let awaiting_votes = s.awaiting_votes.clone();
                let my_node_id = s.my_node_id.clone();
                let cluster = s.cluster.clone();
                let replication_degree = s.replication_degree;
                let all_senders = s.senders.clone();

                // Spawn coordinator task
                // handles local prepare, sends
                // Prepare, and waits for votes without blocking the
                // local loop
                tokio::spawn(async move {
                    // ── Local prepare (inline, no separate spawn) ─────
                    let local_voted = if let Some(mut entries) = local_entries {
                        entries.sort_by_key(|(_, idx)| *idx);

                        let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>> =
                            HashMap::new();
                        let mut lock_failed = false;
                        for (pair, idx) in &entries {
                            if !guards.contains_key(idx) {
                                let stripe = db.get_stripe_by_index(*idx);
                                match stripe.try_lock_owned() {
                                    Ok(guard) => {
                                        debug!(
                                            "Local prepare try_lock successful for stripe index {}, key {:?}, tx {}",
                                            idx, pair.key, tx_id
                                        );
                                        guards.insert(*idx, guard);
                                    }
                                    Err(_) => {
                                        debug!("Local prepare try_lock failed for stripe index {}, key {:?}, tx {}", idx, pair.key,tx_id);
                                        lock_failed = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if lock_failed {
                            false
                        } else {
                            debug!("Getting lock on pending_prepares for {tx_id}");
                            pending_prepares.lock().await.insert(
                                tx_id,
                                PendingTx {
                                    pairs: entries,
                                    guards,
                                },
                            );
                            debug!("Added {tx_id} to pending_prepares");
                            true
                        }
                    } else {
                        true // no local entries to prepare
                    };

                    // Send Prepare to remote primaries
                    for (primary, msg) in &remote_prepare_msgs {
                        if let Some(sender) = all_senders.get(primary) {
                            let _ = sender.send(msg.clone()).await;
                        }
                    }

                    // Collect remote votes (with timeout)
                    // true if we receive enough votes, false otherwise
                    let remote_ok = if num_remote == 0 {
                        true
                    } else {
                        // true if we receive enough votes, false otherwise
                        let collect_result =
                            tokio::time::timeout(COORDINATOR_VOTE_TIMEOUT, async {
                                for _ in 0..num_remote {
                                    match vote_rx.recv().await {
                                        Some(true) => {}
                                        _ => return false,
                                    }
                                }
                                true
                            })
                            .await;
                        if let Ok(true) = collect_result {
                            debug!("Got enough remote votes for tri_put tx {}", tx_id);
                            true
                        } else {
                            debug!("Did not get enough remote votes for tri_put tx {}", tx_id);
                            false
                        }
                    };

                    debug!("Getting lock on awaiting_votes for {tx_id}");
                    awaiting_votes.lock().await.remove(&tx_id);
                    debug!("Removed {} from awaiting_votes", tx_id);

                    let all_prepared = local_voted && remote_ok;

                    if all_prepared {
                        // Apply local changes (if any) and collect pairs to replicate
                        let local_to_replicate = {
                            let mut pending = pending_prepares.lock().await;
                            if let Some(mut tx) = pending.remove(&tx_id) {
                                debug!("removed {} from pending_prepares", tx_id);
                                for (pair, stripe_idx) in &tx.pairs {
                                    if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                                        debug!(
                                            "Inserting key {:?}, val {:?} for tx {} into guard",
                                            pair.key,
                                            pair.val.clone(),
                                            tx_id
                                        );
                                        guard.insert(pair.key, pair.val.clone());
                                    } else {
                                        warn!(
                                            "stripe {} not found in guards for tx {}",
                                            stripe_idx, tx_id
                                        );
                                    }
                                }
                                let to_replicate: Vec<KVPair<K, V>> =
                                    tx.pairs.iter().map(|(p, _)| p.clone()).collect();
                                Some(to_replicate)
                            } else {
                                None
                            }
                        };

                        // Send Commit BEFORE ReplicaPut so remote
                        // participants release their locks promptly
                        for primary in &remote_primaries {
                            if let Some(sender) = all_senders.get(primary) {
                                let msg = PeerMessage::Commit { tx_id };
                                let _ = sender.send(msg).await;
                            }
                        }

                        // Replicate local entries to non-primary replicas
                        if let Some(to_replicate) = local_to_replicate {
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

                    // Send PutResponse BEFORE ReplicaPut so the requester
                    // isn't blocked waiting behind replica writes
                    if let Some(sender) = senders.get(&from) {
                        let _ = sender.send(resp).await;
                    }

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
                });
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
                                    debug!("Inserting stripe index {idx} to guard for tx {tx_id}");
                                    guards.insert(*idx, guard);
                                }
                                Err(_) => {
                                    debug!(
                                        "2PC prepare: Prepare try_lock failed for tx {} on stripe {}",
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
                        debug!("2pc prepare: getting lock on pending_prepares for {tx_id}");
                        pending_prepares.lock().await.insert(
                            tx_id,
                            PendingTx {
                                pairs: entries,
                                guards,
                            },
                        );
                        debug!("2PC prepare: Adding tx {tx_id} to pending_prepares");

                        if let Some(sender) = senders.get(&from) {
                            let resp = PeerMessage::VotePrepared { tx_id };
                            let _ = sender.send(resp).await;
                        }
                    }
                });
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
                debug!("Attempting to commit {tx_id}");
                let pending_prepares = s.pending_prepares.clone();
                let my_node_id = s.my_node_id.clone();
                let cluster = s.cluster.clone();
                let replication_degree = s.replication_degree;
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    debug!("2pc commit: getting lock on pending_prepares for {tx_id}...");
                    let mut pending = pending_prepares.lock().await;
                    debug!("2pc commit: got lock on pending_prepares for {tx_id}");
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
