use crate::{
    key_replica_indices, KVPair, NodeId, PeerMessage, PendingTx, Shared, COORDINATOR_VOTE_TIMEOUT,
};
use anyhow::{anyhow, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::{mpsc, oneshot, OwnedMutexGuard};
use tracing::{debug, error, warn};

pub(crate) async fn handle_local_get<K, V>(
    s: &Arc<Shared<K, V>>,
    key: K,
    response_sender: oneshot::Sender<Option<V>>,
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

    Ok(())
}

pub(crate) async fn handle_local_put<K, V>(
    s: &Arc<Shared<K, V>>,
    pair: KVPair<K, V>,
    response_sender: oneshot::Sender<bool>,
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

    Ok(())
}

pub(crate) async fn handle_local_triput<K, V>(
    s: &Arc<Shared<K, V>>,
    pairs: [KVPair<K, V>; 3],
    response_sender: oneshot::Sender<bool>,
) where
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
    let tx_id: u64 = rand::rng().random();

    // Group pairs by primary, precompute stripe indices
    let mut primary_to_entries: HashMap<NodeId, Vec<(KVPair<K, V>, usize)>> = HashMap::new();
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
            let pairs_only: Vec<KVPair<K, V>> = entries.iter().map(|(p, _)| p.clone()).collect();
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

            let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, V>>> = HashMap::new();
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
            let collect_result = tokio::time::timeout(COORDINATOR_VOTE_TIMEOUT, async {
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
                            warn!("stripe {} not found in guards for tx {}", stripe_idx, tx_id);
                        }
                    }
                    let to_replicate: Vec<KVPair<K, V>> =
                        tx.pairs.iter().map(|(p, _)| p.clone()).collect();
                    Some(to_replicate)
                } else {
                    None
                }
            };

            for primary in &remote_primaries {
                if let Some(sender) = all_senders.get(primary) {
                    let msg = PeerMessage::Commit { tx_id };
                    let _ = sender.send(msg).await;
                }
            }

            // Replicate local entries to non-primary replicas
            if let Some(to_replicate) = local_to_replicate {
                for pair in to_replicate {
                    let replicas =
                        key_replica_indices(&pair.key, cluster.len(), replication_degree);
                    for r_idx in replicas {
                        let replica = &cluster[r_idx];
                        if *replica != my_node_id {
                            if let Some(sender) = all_senders.get(replica) {
                                let msg = PeerMessage::ReplicaPut { pair: pair.clone() };
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

pub(crate) fn handle_peer_put<K, V>(
    s: &Arc<Shared<K, V>>,
    from: NodeId,
    pair: KVPair<K, V>,
    req_id: u64,
) where
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

pub(crate) fn handle_peer_prepare<K, V>(
    s: &Arc<Shared<K, V>>,
    from: NodeId,
    pairs: Vec<KVPair<K, V>>,
    tx_id: u64,
) where
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

pub(crate) fn handle_peer_commit<K, V>(s: &Arc<Shared<K, V>>, tx_id: u64)
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

            let to_replicate: Vec<KVPair<K, V>> = tx.pairs.iter().map(|(p, _)| p.clone()).collect();

            drop(tx);
            drop(pending);

            for pair in to_replicate {
                let replicas = key_replica_indices(&pair.key, cluster.len(), replication_degree);
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
