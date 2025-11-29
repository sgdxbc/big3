use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use big_schema::NodeIndex;
use hashbrown::{HashMap, HashSet};
use log::info;
use ring::digest;
use rocksdb::{DB, WriteBatch};
use tokio::{
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel},
    task::spawn_blocking,
};
use tokio_util::sync::CancellationToken;

use crate::{
    merkle::{MerkleHash, MerkleTree},
    metrics::Latency,
    network::interconnect::{NetworkInterconnectHandle, ReceiveHandle},
    storage3::BigStorageConfig,
};

pub use message::Message;

pub struct ArchiveChannels {
    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,

    pub tx_checkpoint: UnboundedSender<(u64, HashMap<Vec<u8>, Option<Vec<u8>>>)>,
    rx_checkpoint: UnboundedReceiver<(u64, HashMap<Vec<u8>, Option<Vec<u8>>>)>,
}

impl Default for ArchiveChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveChannels {
    pub fn new() -> Self {
        let (tx_message, rx_message) = tokio::sync::mpsc::unbounded_channel();
        let (tx_checkpoint, rx_checkpoint) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx_message,
            rx_message,
            tx_checkpoint,
            rx_checkpoint,
        }
    }

    pub fn receive_handle(&self) -> ReceiveHandle<Message> {
        ReceiveHandle::new(self.tx_message.clone())
    }
}

pub struct ArchiveConfig {
    #[allow(dead_code)]
    stripe_interval: Duration,
}

impl From<&crate::schema::ReplicaTask> for ArchiveConfig {
    fn from(schema: &crate::schema::ReplicaTask) -> Self {
        Self {
            stripe_interval: schema.stripe_interval,
        }
    }
}

pub struct ArchiveTask {
    channels: ArchiveChannels,
    network_interconnect: NetworkInterconnectHandle,

    #[allow(dead_code)]
    config: ArchiveConfig,
    storage_config: BigStorageConfig,
    storing_shards: HashSet<u32>,
    pushing_shards: HashSet<u32>,
    db: Arc<DB>,
    node_index: NodeIndex,

    current_round: u64,
    merkle_roots: Vec<MerkleHash>,
    // shard -> data
    reorder_push_shards: HashMap<u32, Vec<(Vec<u8>, Vec<u8>)>>,

    metrics: ArchiveMetrics,
}

struct ArchiveMetrics {
    round_start: Instant,
    round: Latency,
}

impl ArchiveTask {
    pub fn new(
        channels: ArchiveChannels,
        network_interconnect: NetworkInterconnectHandle,
        config: ArchiveConfig,
        storage_config: BigStorageConfig,
        db: Arc<DB>,
        node_index: NodeIndex,
        merkle_roots: Vec<MerkleHash>,
    ) -> Self {
        let storing_shards = storage_config.storing_shards(node_index);
        let pushing_shards = storage_config.pushing_shards(node_index);
        Self {
            channels,
            network_interconnect,
            config,
            storage_config,
            storing_shards,
            pushing_shards,
            db,
            node_index,
            merkle_roots,
            current_round: 0,
            reorder_push_shards: Default::default(),
            metrics: ArchiveMetrics {
                round_start: Instant::now(),
                round: Latency::new(),
            },
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<()> {
        let (tx, mut rx) = channel(1);
        cancel
            .run_until_cancelled(self.run_inner(tx))
            .await
            .unwrap_or(Ok(()))?;
        let _ = rx.recv().await;
        Ok(())
    }

    async fn run_inner(&mut self, tx_stopped: Sender<()>) -> anyhow::Result<()> {
        let merkle_cf = self.db.cf_handle("merkle").unwrap();
        loop {
            let Some((round, update_table)) = self.channels.rx_checkpoint.recv().await else {
                return Ok(());
            };
            self.current_round = round;
            info!("Starting archive for round {}", self.current_round);
            self.metrics.round_start = Instant::now();
            let mut iter = self.db.raw_iterator();

            let mut shard_updates = vec![HashMap::new(); self.storage_config.num_shards() as usize];
            for (key, value) in update_table {
                let shard = self.storage_config.shard_of_key(&key);
                shard_updates[shard as usize].insert(key, value);
            }

            for shard in 0..self.storage_config.num_shards() {
                let data = if self.storing_shards.contains(&shard) {
                    let prefix = shard.to_be_bytes();
                    iter.seek(prefix);
                    iter.status()?;
                    let mut data = Vec::new();
                    while let Some((key, value)) = iter.item() {
                        let Some(key) = key.strip_prefix(&prefix[..]) else {
                            break;
                        };
                        let mut value = value.to_vec();
                        let index = u32::from_le_bytes(
                            value.split_off(value.len() - 4).try_into().unwrap(),
                        );
                        data.push((key.to_vec(), (value, index)));
                        iter.next();
                        iter.status()?;
                    }
                    data.sort_unstable_by_key(|(_, (_, index))| *index);
                    let data = data
                        .into_iter()
                        .map(|(k, (v, _))| (k, v))
                        .collect::<Vec<_>>();

                    if self.pushing_shards.contains(&shard) {
                        let push_shard = message::PushShard {
                            round: self.current_round,
                            shard,
                            data: data.clone(),
                        };
                        self.network_interconnect
                            .send_to_all(Message::PushShard(push_shard));
                    }

                    data
                } else if let Some(reorder_shard) = self.reorder_push_shards.remove(&shard) {
                    reorder_shard
                } else {
                    loop {
                        let Some(message) = self.channels.rx_message.recv().await else {
                            return Ok(());
                        };
                        match message {
                            Message::PushShard(push_shard) => {
                                if push_shard.round != self.current_round
                                    || push_shard.shard < shard
                                {
                                    continue;
                                }
                                let mut leaves = Vec::new();
                                for (key, value) in &push_shard.data {
                                    let mut context = digest::Context::new(&digest::SHA256);
                                    context.update(&key);
                                    context.update(&value);
                                    context.update(&(self.current_round - 1).to_le_bytes());
                                    let hash = context.finish();
                                    leaves.push(hash.as_ref().try_into().unwrap());
                                }
                                if MerkleTree::new(leaves).root()
                                    != self.merkle_roots[shard as usize]
                                {
                                    anyhow::bail!(
                                        "Received shard {} with invalid merkle root",
                                        shard
                                    );
                                }
                                if push_shard.shard > shard {
                                    self.reorder_push_shards
                                        .insert(push_shard.shard, push_shard.data);
                                    continue;
                                }
                                break push_shard.data;
                            }
                        }
                    }
                };

                let mut data = HashMap::<_, _>::from_iter(data);
                for (key, value) in shard_updates[shard as usize].drain() {
                    if let Some(value) = value {
                        data.insert(key, value);
                    } else {
                        data.remove(&key);
                    }
                }
                let mut data = Vec::from_iter(data);
                data.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));

                let mut leaves = Vec::new();
                for (key, value) in &data {
                    let mut context = digest::Context::new(&digest::SHA256);
                    context.update(&key);
                    context.update(&value);
                    context.update(&self.current_round.to_le_bytes());
                    let hash = context.finish();
                    leaves.push(hash.as_ref().try_into().unwrap());
                }
                let tree = MerkleTree::new(leaves);
                self.merkle_roots[shard as usize] = tree.root();

                let mut stripe_data = bincode::encode_to_vec(&data, bincode::config::standard())?;

                let current_round = self.current_round;
                let k = (self.storage_config.num_faulty_nodes + 1) as usize;
                let db = self.db.clone();
                let recovery_count = self.storage_config.num_faulty_nodes * 2;
                let node_index = self.node_index;
                let tx_stopped = tx_stopped.clone();
                spawn_blocking(move || {
                    let _tx_stopped = tx_stopped;
                    let stripe_data_len = stripe_data.len().next_multiple_of(k * 2);
                    info!(
                        "Archiving round {}, shard {}, data length {} -> {}",
                        current_round,
                        shard,
                        stripe_data.len(),
                        stripe_data_len
                    );
                    stripe_data.resize(stripe_data_len, 0);

                    let encoded_chunks = reed_solomon_simd::encode(
                        k,
                        recovery_count as _,
                        stripe_data.chunks_exact(stripe_data_len / k),
                    )?;
                    info!(
                        "Archived round {}, shard {}, encoded to {} chunks",
                        current_round,
                        shard,
                        encoded_chunks.len()
                    );

                    // TODO do not directly overwrite last round
                    let value = if node_index < k as NodeIndex {
                        stripe_data
                            .chunks_exact(stripe_data_len / k)
                            .nth(node_index as _)
                            .unwrap()
                    } else {
                        &encoded_chunks[node_index as usize - k]
                    };
                    let Some(cf) = db.cf_handle("archive") else {
                        anyhow::bail!("archive column family not found");
                    };
                    db.put_cf(cf, format!("{shard}.{}", node_index), value)?;
                    info!(
                        "Archived round {}, shard {}, data size {}",
                        current_round,
                        shard,
                        value.len()
                    );
                    anyhow::Ok(())
                })
                .await??;

                if self.storing_shards.contains(&shard) {
                    let mut write_batch = WriteBatch::new();
                    for (i, (key, mut value)) in data.into_iter().enumerate() {
                        value.extend_from_slice(&(i as u32).to_le_bytes());
                        write_batch.put(&key, &value);
                    }
                    self.db.write(write_batch)?;
                    let tree_bytes = bincode::encode_to_vec(&tree, bincode::config::standard())?;
                    self.db
                        .put_cf(merkle_cf, shard.to_be_bytes(), &tree_bytes[..])?;
                }
            }

            self.metrics.round += self.metrics.round_start.elapsed();
        }
    }
}

mod message {
    use bincode::{Decode, Encode};

    #[derive(Decode, Encode)]
    pub enum Message {
        PushShard(PushShard),
        // Vote(Vote),
    }

    #[derive(Decode, Encode)]
    pub struct PushShard {
        pub round: u64,
        pub shard: u32,
        pub data: Vec<(Vec<u8>, Vec<u8>)>,
    }

    // #[derive(Decode, Encode)]
    // pub struct Vote {
    //     pub round: u64,
    //     pub node_index: NodeIndex,
    // }
}
