use std::{net::IpAddr, time::Duration};

use serde::{Deserialize, Serialize};

// payload of `/load`
#[derive(Clone, Serialize, Deserialize)]
pub enum Task {
    Replica(ReplicaTask),
    Client(ClientTask),
    Prefill(PrefillTask),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplicaTask {
    pub ips: Vec<IpAddr>,
    pub config: ReplicaConfig,
    pub node_index: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientTask {
    pub ips: Vec<IpAddr>,
    pub config: ClientConfig,
    pub worker_config: ClientWorkerConfig,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrefillTask {
    pub num_keys: u64,
}

// response of `/scrape`
#[derive(Serialize, Deserialize)]
pub struct Scrape {
    pub interval: Duration,
    #[serde(with = "serde_bytes")]
    pub latency_histogram: Vec<u8>,
}

// response of `/stop`
#[derive(Serialize, Deserialize)]
pub enum Stopped {
    BeforeStart,
    Replica,
    Client,
}

// inner types
pub type NodeIndex = u16;

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientWorkerConfig {
    pub num_concurrent: usize,
    pub num_keys: u64,
    pub read_ratio: f64,
}
