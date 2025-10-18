use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem::take,
};

use bincode::{Decode, Encode};
use log::{debug, trace, warn};
use sha2::{Digest as _, Sha256};

use crate::types::{NodeIndex, Request};

pub type Round = u64;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Debug for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockHash(0x{}...)", &self.to_hex()[..8])
    }
}

#[derive(Clone)]
pub struct NarwhalConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,
    // we adopt a simplified garbage collection rule of the Bullshark paper
    // when the leader (anchor) is delivered at round r, garbage collect up to round
    // r - garbage_collection_depth
    garbage_collection_depth: Option<Round>,
}

impl From<&crate::schema::ReplicaConfig> for NarwhalConfig {
    fn from(config: &crate::schema::ReplicaConfig) -> Self {
        Self {
            num_node: config.num_nodes,
            num_faulty_node: config.num_faulty_nodes,
            garbage_collection_depth: Some(10),
        }
    }
}

impl NarwhalConfig {
    fn is_bullshark_leader(&self, node_index: NodeIndex, round: Round) -> bool {
        round % 2 == 0 && node_index == (round / 2 % self.num_node as Round) as NodeIndex
    }
}

#[derive(Debug, Clone)]
pub struct Block {
    pub round: Round,
    pub node_index: NodeIndex,
    pub links: Vec<BlockHash>,
    pub txns: Vec<Request>,
}

impl Block {
    pub fn hash(&self) -> BlockHash {
        let mut hasher = Sha256::new();
        hasher.update(self.round.to_le_bytes());
        hasher.update(self.node_index.to_le_bytes());
        for BlockHash(link_hash) in &self.links {
            hasher.update(link_hash)
        }
        for request in &self.txns {
            hasher.update(request.client_id.to_le_bytes());
            hasher.update(request.client_seq.to_le_bytes());
            hasher.update(&request.command);
        }
        BlockHash(hasher.finalize().into())
    }

    fn from_network(block: &message::Block) -> Self {
        Self {
            round: block.round,
            node_index: block.creator_index,
            links: block.certs.iter().map(|cert| cert.block_hash).collect(),
            txns: block.txns.clone(),
        }
    }
}

pub trait NarwhalContext {
    fn send(&mut self, node_index: NodeIndex, message: message::Message);
    fn send_to_all(&mut self, message: message::Message);
    fn deliver(&mut self, block: Block);
}

pub struct Narwhal<C> {
    context: C,
    config: NarwhalConfig,
    node_index: NodeIndex,

    round: Round,
    block_hash: Option<BlockHash>, // None if proposal for current round has certified
    txn_pool: Vec<Request>,
    block_oks: HashMap<NodeIndex, message::BlockOk>,
    certs: HashMap<Round, HashMap<NodeIndex, message::Cert>>,
    reorder_validate: HashMap<Round, Vec<(NodeIndex, BlockHash)>>,

    certifying_blocks: HashMap<BlockHash, Block>,
    delivered: HashMap<Round, HashSet<BlockHash>>,
    reorder_blocks: HashMap<BlockHash, Vec<Block>>, // missing parent -> children
    reorder_certified: HashSet<BlockHash>,
}

impl<C> Narwhal<C> {
    pub fn new(context: C, config: NarwhalConfig, node_index: NodeIndex) -> Self {
        let (
            round,
            block_hash,
            txn_pool,
            block_oks,
            certs,
            reorder_validate,
            certifying_blocks,
            delivered,
            reorder_blocks,
            reorder_certified,
        ) = Default::default();
        Self {
            context,
            config,
            node_index,
            round,
            block_hash,
            txn_pool,
            block_oks,
            certs,
            reorder_validate,
            certifying_blocks,
            delivered,
            reorder_blocks,
            reorder_certified,
        }
    }
}

impl<C: NarwhalContext> Narwhal<C> {
    pub fn init(&mut self) {
        self.propose();
    }

    pub fn on_request(&mut self, request: Request) {
        self.txn_pool.push(request);
    }

    pub fn on_message(&mut self, message: message::Message) {
        match message {
            message::Message::Block(network_block) => {
                let block = Block::from_network(&network_block);
                self.validate(&block);
                self.certifying(block)
            }
            message::Message::BlockOk(block_ok) => {
                assert!(block_ok.round <= self.round);
                if block_ok.round == self.round {
                    self.insert_block_ok(block_ok)
                }
            }
            message::Message::Cert(cert) => self.handle_cert(cert),
        }
    }

    fn handle_cert(&mut self, cert: message::Cert) {
        self.certified(cert.block_hash);
        let cert_round = cert.round;
        if cert_round < self.round {
            return;
        }
        let round_certs = self.certs.entry(cert_round).or_default();
        round_certs.insert(cert.creator_index, cert);
        if round_certs.len() >= (self.config.num_node - self.config.num_faulty_node) as usize
        // TODO may need to restrict DAG shape
        {
            if cert_round > self.round {
                warn!(
                    "fast-forwarding from round {} to {}",
                    self.round,
                    cert_round + 1
                );
            }
            self.round = cert_round + 1;
            trace!("[{}] advanced to round {}", self.node_index, self.round);
            self.certs.retain(|&r, _| r >= cert_round);
            self.propose();
            self.reorder_validate.retain(|&r, _| r >= self.round);
            if let Some(pending) = self.reorder_validate.remove(&self.round) {
                for (node_index, block_hash) in pending {
                    self.validate2(node_index, block_hash)
                }
            }
        }
    }

    fn propose(&mut self) {
        if let Some(block_hash) = self.block_hash {
            debug!("[{}] interrupted proposal {block_hash:?}", self.node_index);
            self.block_oks.clear()
        }
        let certs = if self.round == 0 {
            Default::default()
        } else {
            self.certs.remove(&(self.round - 1)).unwrap()
        };
        assert!(certs.iter().all(|(_, cert)| cert.round == self.round - 1));
        // TODO limit number of txns
        let txns = self.txn_pool.clone();
        self.txn_pool.clear();
        let network_block = message::Block {
            round: self.round,
            creator_index: self.node_index,
            certs: certs.into_values().collect(),
            txns,
        };
        let block = Block::from_network(&network_block);
        self.context
            .send_to_all(message::Message::Block(network_block));
        self.block_hash = Some(block.hash());
        self.validate(&block);
        self.certifying(block)
    }

    fn validate(&mut self, block: &Block) {
        if block.round < self.round {
            trace!(
                "[{}] ignoring old block for round {} < {}",
                self.node_index, block.round, self.round
            );
            return;
        }
        // TODO verify integrity
        let block_hash = block.hash();
        if block.round == self.round {
            self.validate2(block.node_index, block_hash)
        } else {
            self.reorder_validate
                .entry(block.round)
                .or_default()
                .push((block.node_index, block_hash))
        }
    }

    fn validate2(&mut self, node_index: NodeIndex, block_hash: BlockHash) {
        // TODO verify non-equivocation
        let block_ok = message::BlockOk {
            hash: block_hash,
            round: self.round,
            creator_index: node_index,
            validator_index: self.node_index,
            sig: vec![], // TODO
        };
        if node_index == self.node_index {
            self.insert_block_ok(block_ok)
        } else {
            self.context
                .send(node_index, message::Message::BlockOk(block_ok));
        }
    }

    fn insert_block_ok(&mut self, block_ok: message::BlockOk) {
        assert!(block_ok.round == self.round);
        let Some(block_hash) = self.block_hash else {
            return;
        };
        if block_ok.hash != block_hash || block_ok.creator_index != self.node_index {
            warn!("invalid BlockOk for round {}", block_ok.round);
            return;
        }
        // TODO verify signature
        self.block_oks.insert(block_ok.validator_index, block_ok);
        if self.block_oks.len() == (self.config.num_node - self.config.num_faulty_node) as usize {
            trace!(
                "[{}] block {:?} certified for round {}",
                self.node_index, block_hash, self.round
            );
            let cert = message::Cert {
                round: self.round,
                creator_index: self.node_index,
                block_hash: self.block_hash.take().unwrap(),
                sigs: take(&mut self.block_oks)
                    .into_iter()
                    .map(|(node_index, block_ok)| (node_index, block_ok.sig))
                    .collect(),
            };
            self.context
                .send_to_all(message::Message::Cert(cert.clone()));
            self.handle_cert(cert)
        }
    }

    fn certifying(&mut self, block: Block) {
        if self.reorder_certified.remove(&block.hash()) {
            self.may_deliver(block)
        } else {
            let block_hash = block.hash();
            self.certifying_blocks.insert(block_hash, block);
        }
    }

    fn certified(&mut self, block_hash: BlockHash) {
        let Some(block) = self.certifying_blocks.remove(&block_hash) else {
            self.reorder_certified.insert(block_hash);
            return;
        };
        self.may_deliver(block)
    }

    fn may_deliver(&mut self, block: Block) {
        for &link in &block.links {
            if !self
                .delivered
                .get(&(block.round - 1))
                .is_some_and(|delivered| delivered.contains(&link))
            {
                self.reorder_blocks.entry(link).or_default().push(block);
                return;
            }
        }
        // first perform bookkeeping that access block fields
        let block_hash = block.hash();
        let round_delivered = self.delivered.entry(block.round).or_default();
        round_delivered.insert(block_hash);
        if let Some(depth) = self.config.garbage_collection_depth
            && block.round >= depth
            && self
                .config
                .is_bullshark_leader(block.node_index, block.round)
        {
            let gc_round = block.round - depth;
            self.delivered.retain(|&r, _| r > gc_round);
            self.certifying_blocks.retain(|_, b| b.round > gc_round)
            // we do not perform garbage collection in `reorder_blocks` and `reorder_certified`
            // because the relevant missing blocks are _secured_ by a quorum certificate so they
            // will eventually appear
        }
        // then depart the block
        self.context.deliver(block);
        if let Some(blocks) = self.reorder_blocks.remove(&block_hash) {
            for block in blocks {
                self.may_deliver(block)
            }
        }
    }
}

pub mod message {
    use bincode::{Decode, Encode};

    use crate::types::Request;

    use super::{BlockHash, NodeIndex, Round};

    #[derive(Debug, Encode, Decode)]
    pub enum Message {
        Block(Block),
        BlockOk(BlockOk),
        Cert(Cert),
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Block {
        pub round: Round,
        pub creator_index: NodeIndex,
        pub certs: Vec<Cert>,
        pub txns: Vec<Request>,
    }

    #[derive(Debug, Encode, Decode)]
    pub struct BlockOk {
        pub hash: BlockHash,
        pub round: Round,
        pub creator_index: NodeIndex,
        pub validator_index: NodeIndex,
        pub sig: Vec<u8>, // TODO
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Cert {
        pub block_hash: BlockHash,
        pub round: Round,
        pub creator_index: NodeIndex,
        pub sigs: Vec<(NodeIndex, Vec<u8>)>,
    }
}
