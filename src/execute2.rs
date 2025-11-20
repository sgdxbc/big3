use std::mem::take;

use bincode::{Decode, Encode};
use log::info;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::{
    spawn,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::{ClientId, ClientSeq, NodeIndex, Reply},
    consensus::{Block, DeliverHandle},
    execute::{AbstractExecute, AbstractOp},
    network::server::NetworkOutgoingHandle,
    storage2::{FetchedHandle, PostDoneHandle},
};

pub struct ExecuteChannels {
    pub tx_blocks: UnboundedSender<Vec<Block>>,
    rx_blocks: UnboundedReceiver<Vec<Block>>,

    rx_fetched: UnboundedReceiver<FxHashMap<Vec<u8>, Option<Vec<u8>>>>,
    tx_fetched: UnboundedSender<FxHashMap<Vec<u8>, Option<Vec<u8>>>>,

    tx_post_done: UnboundedSender<()>,
    rx_post_done: UnboundedReceiver<()>,
}

pub struct FetchHandle {
    pub tx_keys: UnboundedSender<FxHashSet<Vec<u8>>>,
}

pub struct PostHandle {
    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

impl ExecuteChannels {
    pub fn new() -> Self {
        let (tx_blocks, rx_blocks) = unbounded_channel();
        let (tx_fetched, rx_fetched) = unbounded_channel();
        let (tx_post_done, rx_post_done) = unbounded_channel();
        Self {
            tx_blocks,
            rx_blocks,
            tx_fetched,
            rx_fetched,
            tx_post_done,
            rx_post_done,
        }
    }

    pub fn deliver_handle(&self) -> DeliverHandle {
        DeliverHandle {
            tx_blocks: self.tx_blocks.clone(),
        }
    }

    pub fn fetched_handle(&self) -> FetchedHandle {
        FetchedHandle {
            tx_fetched: self.tx_fetched.clone(),
        }
    }

    pub fn post_done_handle(&self) -> PostDoneHandle {
        PostDoneHandle {
            tx_post_done: self.tx_post_done.clone(),
        }
    }
}

pub struct ExecuteTask<E: AbstractExecute> {
    pub channels: ExecuteChannels,
    fetch_handle: FetchHandle,
    post_handle: PostHandle,
    network_outgoing: NetworkOutgoingHandle,

    node_index: NodeIndex,
    num_faulty_nodes: NodeIndex,

    state: E,
    fetching_requests: Vec<(E::Op, ClientId, ClientSeq)>,
    reply_flag: NodeIndex,
}

impl<E: AbstractExecute> ExecuteTask<E> {
    pub fn new(
        channels: ExecuteChannels,
        fetch_handle: FetchHandle,
        post_handle: PostHandle,
        network_outgoing: NetworkOutgoingHandle,
        node_index: NodeIndex,
        num_faulty_nodes: NodeIndex,
        state: E,
    ) -> Self {
        Self {
            channels,
            fetch_handle,
            post_handle,
            network_outgoing,
            node_index,
            num_faulty_nodes,
            state,
            fetching_requests: Default::default(),
            reply_flag: node_index,
        }
    }
}

impl<E: AbstractExecute> ExecuteTask<E>
where
    E::Op: Decode<()>,
    E::Res: Encode,
{
    fn handle_blocks(&mut self, blocks: Vec<Block>) {
        self.parse_blocks(blocks);
    }

    fn parse_blocks(&mut self, blocks: Vec<Block>) {
        assert!(self.fetching_requests.is_empty());

        let mut keys = FxHashSet::default();
        for block in blocks {
            for request in block.txns {
                let op = bincode::decode_from_slice::<E::Op, _>(
                    &request.command,
                    bincode::config::standard(),
                )
                .unwrap()
                .0;
                for key in op.read_set() {
                    keys.insert(key);
                }
                self.fetching_requests
                    .push((op, request.client_id, request.client_seq));
            }
        }
        if self.fetching_requests.is_empty() {
            return;
        }
        info!(
            "request number {} fetching {} keys",
            self.fetching_requests.len(),
            keys.len()
        );
        let _ = self.fetch_handle.tx_keys.send(keys);
    }

    fn handle_fetched(&mut self, mut state: FxHashMap<Vec<u8>, Option<Vec<u8>>>) {
        info!("fetched state with {} entries", state.len());

        let fetching_requests = take(&mut self.fetching_requests);

        // update_intersection_move(&mut state, take(&mut self.last_state));
        let mut updates = Vec::new();
        for (op, client_id, client_seq) in fetching_requests {
            let (res, op_updates) = self.state.execute(op, &state);
            updates.extend(op_updates.clone());
            state.extend(op_updates);

            if self.reply_flag <= self.num_faulty_nodes {
                let reply = Reply {
                    client_seq,
                    res: bincode::encode_to_vec(res, bincode::config::standard()).unwrap(),
                    node_index: self.node_index,
                };
                let _ = self.network_outgoing.send_message(client_id, reply);
            }
            self.reply_flag = (self.reply_flag + 1) % (2 * self.num_faulty_nodes + 1);
        }
        let _ = self.post_handle.tx_post.send(updates);
    }

    fn handle_post_done(&mut self) {
        //
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()>
    where
        E: 'static + Send,
        E::Op: 'static + Send,
    {
        spawn(async move { cancel.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some(blocks) = self.channels.rx_blocks.recv().await {
            self.handle_blocks(blocks);
            if self.fetching_requests.is_empty() {
                continue;
            }
            let Some(state) = self.channels.rx_fetched.recv().await else {
                return;
            };
            self.handle_fetched(state);
            let Some(()) = self.channels.rx_post_done.recv().await else {
                return;
            };
            self.handle_post_done();
        }
    }
}

pub fn update_intersection_move<K, V>(a: &mut FxHashMap<K, V>, b: FxHashMap<K, V>)
where
    K: Eq + std::hash::Hash,
{
    // If B is smaller, just move from B into A
    if b.len() <= a.len() {
        for (k, v_b) in b {
            if let Some(v_a) = a.get_mut(&k) {
                *v_a = v_b;
            }
        }
    } else {
        // If A is smaller, remove from a temporary mutable B
        let mut b = b;
        for (k, v_a) in a.iter_mut() {
            if let Some(v_b) = b.remove(k) {
                *v_a = v_b;
            }
        }
        // `b` is dropped here
    }
}
