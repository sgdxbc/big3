use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum Task {
    Replica,
    Client(ClientTask),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientTask {
    pub addrs: Vec<SocketAddr>,
    pub config: ClientConfig,
    pub worker_config: ClientWorkerConfig,
}

#[derive(Serialize, Deserialize)]
pub enum Stopped {
    Replica,
    Client,
}

pub type NodeIndex = u16;

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClientWorkerConfig {
    pub num_concurrent: usize,
}
