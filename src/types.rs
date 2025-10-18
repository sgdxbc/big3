use bincode::{Decode, Encode};

pub type NodeIndex = big_schema::NodeIndex;
pub type ClientId = u32;
pub type ClientSeq = u64;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request {
    pub client_id: ClientId,
    pub client_seq: ClientSeq,
    pub command: Vec<u8>,
}

#[derive(Debug, Encode, Decode)]
pub struct Reply {
    pub client_seq: ClientSeq,
    pub res: Vec<u8>,
    pub node_index: NodeIndex,
}
