use crate::KVPair;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::oneshot;

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
