// a (highly) simplified implementation of Bullshark consensus
// this implementation assumes exactly 2f+1 operational nodes. in each round,
// every node proposes a block, collect and broadcast quorum certificates for
// the block, and can only proceed to the next round after collecting quorum
// certificates for _all_ (other) nodes' blocks
// some noticeable characteristics:
// * all blocks are committed in a deterministic way. basically, at round 1,
//   the leader block of round 0 is committed; at round 3, the leader block of
//   round 2 and every (remaining block) at round 0 and 1 at committed, etc.
//   there will be no stalled blocks at all, and every transaction that has been
//   included in a proposed block will be committed (soon), and the garbage
//   collection can be omitted
// * the back pressure is global. if any node apply back pressure to consensus
//   (probably due to slow storage), all nodes cannot proceed to the next round

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    mem::take,
    time::Instant,
};

use bincode::{Decode, Encode};
use hdrhistogram::Histogram;
use log::{info, trace};
use ring::digest;

use crate::{
    common::{NodeIndex, Request, RequestId},
    metrics::Latency,
};

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
pub struct BullsharkConfig {
    num_node: NodeIndex,
    num_faulty_node: NodeIndex,
    max_concurrent_executing: usize,
}

impl From<&crate::schema::ReplicaTask> for BullsharkConfig {
    fn from(schema: &crate::schema::ReplicaTask) -> Self {
        Self {
            num_node: schema.config.num_nodes,
            num_faulty_node: schema.config.num_faulty_nodes,
            max_concurrent_executing: schema.max_concurrent_executing,
        }
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
        let mut hasher = digest::Context::new(&digest::SHA256);
        hasher.update(&self.round.to_le_bytes());
        hasher.update(&self.node_index.to_le_bytes());
        for BlockHash(link_hash) in &self.links {
            hasher.update(link_hash)
        }
        for request in &self.txns {
            hasher.update(&request.client_id.to_le_bytes());
            hasher.update(&request.client_seq.to_le_bytes());
            hasher.update(&request.command);
        }
        BlockHash(hasher.finish().as_ref().try_into().unwrap())
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

pub trait BullsharkContext {
    fn send(&mut self, node_index: NodeIndex, message: message::Message);
    fn send_to_all(&mut self, message: message::Message);
    fn output(&mut self, blocks: Vec<Block>) -> OutputId;
}

pub type OutputId = RequestId;

pub struct Bullshark<C> {
    context: C,
    config: BullsharkConfig,
    node_index: NodeIndex,

    // propose states
    round: Round,
    txn_pool: Vec<Request>,
    block_oks: HashMap<NodeIndex, message::BlockOk>,
    // mostly only certs of current round (i.e. previous round at the point of proposing) are
    // relevant. however, if the proposal is delayed until after starting to receive certs of the
    // next (current) round, we need to keep the certs of two rounds simultaneously
    certs: HashMap<Round, HashMap<NodeIndex, message::Cert>>,

    // validate states
    reorder_validate: HashMap<Round, Vec<Block>>,

    // deliver states
    certifying: HashMap<BlockHash, Block>,
    committing: HashMap<Round, BTreeMap<NodeIndex, Block>>,

    executing: HashSet<OutputId>,
    execute_backpressured: bool,

    metrics: BullsharkMetrics,
}

struct BullsharkMetrics {
    proposed_block_size: Histogram<u64>,
    output_block_size: Histogram<u64>,
    back_pressure_start: Option<Instant>,
    back_pressure: Latency,
    round_start: Instant,
    round: Latency,
}

impl Default for BullsharkMetrics {
    fn default() -> Self {
        Self {
            proposed_block_size: Histogram::new(3).unwrap(),
            output_block_size: Histogram::new(3).unwrap(),
            back_pressure_start: None,
            back_pressure: Latency::new(),
            round_start: Instant::now(),
            round: Latency::new(),
        }
    }
}

impl<C> Bullshark<C> {
    pub fn new(context: C, config: BullsharkConfig, node_index: NodeIndex) -> Self {
        let (
            (
                round,
                txn_pool,
                block_oks,
                certs,
                certifying,
                committing,
                reorder_validate,
                executing,
                execute_backpressured,
            ),
            metrics,
        ) = Default::default();
        Self {
            context,
            config,
            node_index,
            round,
            txn_pool,
            block_oks,
            certs,
            reorder_validate,
            certifying,
            committing,
            executing,
            execute_backpressured,
            metrics,
        }
    }

    const MAX_BLOCK_TXNS: usize = 10_000;
}

impl<C: BullsharkContext> Bullshark<C> {
    pub fn start(&mut self) {
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
                assert_eq!(block_ok.round, self.round);
                self.insert_block_ok(block_ok)
            }
            message::Message::Cert(cert) => self.handle_cert(cert),
        }
    }

    pub fn log_metrics(&self) {
        info!(
            "bullshark metrics:\n\
            proposed block size: avg {:.0} req, p50 {:.0} req, p95 {:.0} req, p99 {:.0} req\n\
            output block size: avg {:.0} req, p50 {:.0} req, p95 {:.0} req, p99 {:.0} req\n\
            throttle time: {}\n\
            rounds completed: {}",
            self.metrics.proposed_block_size.mean(),
            self.metrics.proposed_block_size.value_at_quantile(0.5),
            self.metrics.proposed_block_size.value_at_quantile(0.95),
            self.metrics.proposed_block_size.value_at_quantile(0.99),
            self.metrics.output_block_size.mean(),
            self.metrics.output_block_size.value_at_quantile(0.5),
            self.metrics.output_block_size.value_at_quantile(0.95),
            self.metrics.output_block_size.value_at_quantile(0.99),
            self.metrics.back_pressure,
            self.metrics.round,
        );
    }

    fn handle_cert(&mut self, cert: message::Cert) {
        self.certified(cert.block_hash);
        assert_eq!(cert.round, self.round);
        let round_certs = self.certs.entry(self.round).or_default();
        round_certs.insert(cert.creator_index, cert);
        if round_certs.len() < (self.config.num_node - self.config.num_faulty_node) as usize {
            return;
        }

        self.round += 1;
        trace!("[{}] moving to round {}", self.node_index, self.round);
        self.metrics.round += self.metrics.round_start.elapsed();
        self.metrics.round_start = Instant::now();

        if self.executing.len() <= self.config.max_concurrent_executing {
            self.propose();
        } else {
            self.execute_backpressured = true;
            self.metrics
                .back_pressure_start
                .get_or_insert_with(Instant::now);
        }

        if let Some(blocks) = self.reorder_validate.remove(&self.round) {
            for block in blocks {
                self.validate(&block);
            }
        }
    }

    fn propose(&mut self) {
        trace!(
            "[{}] proposing for round {} pool size {}",
            self.node_index,
            self.round,
            self.txn_pool.len()
        );
        let certs = if self.round != 0 {
            let certs = self.certs.remove(&(self.round - 1)).unwrap();
            assert!(certs.iter().all(|(_, cert)| cert.round == self.round - 1));
            assert!(certs.len() >= (self.config.num_node - self.config.num_faulty_node) as usize);
            certs
        } else {
            Default::default()
        };
        let network_block = message::Block {
            round: self.round,
            creator_index: self.node_index,
            certs: certs.into_values().collect(),
            txns: self
                .txn_pool
                .drain(..Self::MAX_BLOCK_TXNS.min(self.txn_pool.len()))
                .collect(),
        };
        if !network_block.txns.is_empty() {
            self.metrics.proposed_block_size += network_block.txns.len() as u64;
        }

        let block = Block::from_network(&network_block);
        self.context
            .send_to_all(message::Message::Block(network_block));
        self.block_oks.clear();

        self.validate(&block);
        self.certifying(block)
    }

    fn validate(&mut self, block: &Block) {
        assert!(block.round == self.round || block.round == self.round + 1);
        // TODO verify integrity
        if block.round != self.round {
            self.reorder_validate
                .entry(block.round)
                .or_default()
                .push(block.clone());
            return;
        }

        let block_hash = block.hash();
        self.validate2(block.node_index, block_hash)
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
        assert_eq!(block_ok.round, self.round);
        assert_eq!(block_ok.creator_index, self.node_index);
        // TODO verify signature
        let block_hash = block_ok.hash;
        self.block_oks.insert(block_ok.validator_index, block_ok);
        if self.block_oks.len() == (self.config.num_node - self.config.num_faulty_node) as usize {
            trace!(
                "[{}] block {:?} certified for round {}",
                self.node_index, block_hash, self.round
            );
            let cert = message::Cert {
                round: self.round,
                creator_index: self.node_index,
                block_hash,
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
        self.certifying.insert(block.hash(), block);
    }

    fn certified(&mut self, block_hash: BlockHash) {
        let block = self.certifying.remove(&block_hash).unwrap();
        self.deliver(block)
    }

    fn deliver(&mut self, block: Block) {
        trace!(
            "[{}] delivering block {:?} ({}, {})",
            self.node_index,
            block.hash(),
            block.round,
            block.node_index
        );
        assert_eq!(block.round, self.round);
        let round_delivered = self.committing.entry(block.round).or_default();
        round_delivered.insert(block.node_index, block);

        if self.round.is_multiple_of(2)
            || round_delivered.len() != (self.config.num_faulty_node + 1) as usize
        {
            return;
        }

        // at this point, every block at round-1 has f+1 links symmetrically. since they also share
        // the same causal dependencies (i.e. all 2f+1 nodes from round-2), it's not significant
        // which block is the leader. here we just pick block proposed by node 0 as the leader for
        // simplicity

        let mut blocks = Vec::new();
        if self.round > 1 {
            let blocks1 = self.committing.remove(&(self.round - 3)).unwrap();
            assert_eq!(
                blocks1.len(),
                (self.config.num_node - self.config.num_faulty_node - 1) as usize
            );
            blocks.extend(blocks1.into_values());
            let blocks2 = self.committing.remove(&(self.round - 2)).unwrap();
            assert_eq!(
                blocks2.len(),
                (self.config.num_node - self.config.num_faulty_node) as usize
            );
            blocks.extend(blocks2.into_values());
        }
        blocks.push(
            self.committing
                .get_mut(&(self.round - 1))
                .unwrap()
                .remove(&0)
                .unwrap(),
        );

        let output_id = self.context.output(blocks);
        self.executing.insert(output_id);
    }

    pub fn on_output_response(&mut self, output_id: OutputId) {
        let removed = self.executing.remove(&output_id);
        assert!(removed);
        trace!(
            "[{}] output {} completed, inflight {}",
            self.node_index,
            output_id,
            self.executing.len()
        );

        if self.executing.len() <= self.config.max_concurrent_executing
            && take(&mut self.execute_backpressured)
        {
            if let Some(start) = self.metrics.back_pressure_start.take() {
                self.metrics.back_pressure += start.elapsed();
            }
            self.propose();
        }
    }
}

pub mod message {
    use bincode::{Decode, Encode};

    use crate::common::Request;

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
