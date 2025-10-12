use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum Task {
    Replica,
}

#[derive(Serialize, Deserialize)]
pub enum TaskMetrics {
    Replica,
}

pub type NodeIndex = u16;

#[derive(Serialize, Deserialize)]
pub struct ClientConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
}

#[derive(Serialize, Deserialize)]
pub struct ClientWorkerConfig {
    pub num_concurrent: usize,
}
