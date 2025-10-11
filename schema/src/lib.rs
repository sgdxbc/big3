use serde::{Deserialize, Serialize};

pub type NodeIndex = u16;

#[derive(Serialize, Deserialize)]
pub struct ClientConfig {
    pub num_nodes: NodeIndex,
    pub num_faulty_nodes: NodeIndex,
    pub num_max_ongoing: usize,
}
