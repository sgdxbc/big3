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
    pub node_index: NodeIndex,
    pub num_shards: ShardIndex,
    pub shard_index: ShardIndex,
    pub storage: Storage,
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
    pub num_keys: u64,
    pub storage: Storage,
    pub app: App,
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
    Ycsb,
    Utxo,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UtxoWorkloadConfig {
    pub num_outputs: u64,
}
