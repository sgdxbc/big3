use std::{net::IpAddr, time::Duration};

use serde::{Deserialize, Serialize};

// payload of `/load`
#[derive(Clone, Serialize, Deserialize)]
pub enum Task {
    Replica(ReplicaTask),
    Client(ClientTask),
    Prefill(PrefillTask),
}

pub type ShardIndex = u8;

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplicaTask {
    pub ips: Vec<IpAddr>,
    pub latencies: Option<Vec<Vec<u32>>>,

    pub config: ReplicaConfig,

    pub num_shards: ShardIndex,
    pub num_shard_faulty_nodes: NodeIndex,

    pub shard_index: ShardIndex,
    pub shard_node_index: NodeIndex,

    pub max_concurrent_executing: usize,

    pub storage: Storage,
    pub cache_size: usize,
    pub stripe_interval: Duration,
    pub checkpoint: bool,

    pub app: App,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientTask {
    pub ips: Vec<Vec<IpAddr>>,
    pub config: ClientConfig,
    pub workload_config: ClientWorkloadConfig,
    pub node_index: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrefillTask {
    pub storage: Storage,
    pub app: App,
    pub config: ReplicaConfig,
    pub full: bool,
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
    Replica,
    Client,
}

// inner types
pub type NodeIndex = u16;

#[derive(Clone, Serialize, Deserialize)]
pub struct ReplicaConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
    pub node_index: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    // pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientWorkloadConfig {
    pub num_concurrent: u32,
    pub num_shards: ShardIndex,
    pub app: WorkloadConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Storage {
    Full,
    Big,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum App {
    Ycsb(u64),
    Utxo(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkloadConfig {
    Ycsb(YcsbWorkloadConfig),
    Utxo(UtxoWorkloadConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YcsbWorkloadConfig {
    pub num_keys: u64,
    pub read_ratio: f64,
    pub theta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UtxoWorkloadConfig {
    pub num_outputs: u64,
}
