use std::{
    mem::take,
    sync::Arc,
    thread::available_parallelism,
    time::{Duration, Instant},
};

use big_schema::NodeIndex;
use hashbrown::{HashMap, HashSet};
use log::{debug, info};
use ring::digest;
use rocksdb::{DB, WriteBatch};
use tokio::{
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel},
    task::JoinSet,
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
    pub channels: ArchiveChannels,
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

    pub metrics: ArchiveMetrics,
}

pub struct ArchiveMetrics {
    pub round: Latency,

    pub scan: Latency,
    pub network: Latency,
    pub verify: Latency,
    pub update: Latency,

    merklize: Latency,
    encode: Latency,
    store: Latency,
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
                round: Latency::new(),
                scan: Latency::new(),
                network: Latency::new(),
                verify: Latency::new(),
                update: Latency::new(),
                merklize: Latency::new(),
                encode: Latency::new(),
                store: Latency::new(),
            },
        }
    }

    fn log_metrics(&self) {
        info!(
            "Archive metrics:\nRound time: {}\nScan time: {}\nNetwork time: {}\nVerify time: {}\nUpdate time: {}\nMerklize time: {}\nEncode time: {}\nStore time: {}",
            self.metrics.round,
            self.metrics.scan,
            self.metrics.network,
            self.metrics.verify,
            self.metrics.update,
            self.metrics.merklize,
            self.metrics.encode,
            self.metrics.store,
        );
    }

    pub async fn run(mut self, cancel: CancellationToken) -> anyhow::Result<ArchiveMetrics> {
        let (tx, mut rx) = channel(1);
        cancel
            .run_until_cancelled(self.run_inner(tx, cancel.clone()))
            .await
            .unwrap_or(Ok(()))?;
        let _ = rx.recv().await;
        self.log_metrics();
        Ok(self.metrics)
    }

    async fn run_inner(
        &mut self,
        tx_stopped: Sender<()>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();
        loop {
            let Some((round, update_table)) = self.channels.rx_checkpoint.recv().await else {
                return Ok(());
            };
            assert!(round > self.current_round);
            self.current_round = round;
            info!("Starting archive for round {}", self.current_round);
            let round_start = Instant::now();

            let db = self.db.clone();
            let mut iter = db.raw_iterator();

            let mut shard_updates = vec![HashMap::new(); self.storage_config.num_shards() as usize];
            for (key, value) in update_table {
                let shard = self.storage_config.shard_of_key(&key);
                shard_updates[shard as usize].insert(key, value);
            }

            let batch_size = 40;
            for start_shard in (0..self.storage_config.num_shards()).step_by(batch_size as _) {
                // if start_shard >= 50 {
                //     break;
                // }

                let mut shards = HashMap::new();
                for shard in
                    start_shard..(start_shard + batch_size).min(self.storage_config.num_shards())
                {
                    if self.storing_shards.contains(&shard) {
                        self.scan(&mut iter, &mut shards, shard)?;
                    }
                }

                for shard in
                    start_shard..(start_shard + batch_size).min(self.storage_config.num_shards())
                {
                    let data = if let Some(data) = shards.remove(&shard) {
                        data
                    } else if let Some(reorder_shard) = self.reorder_push_shards.remove(&shard) {
                        reorder_shard
                    } else {
                        loop {
                            let start = Instant::now();
                            let Some(message) = self.channels.rx_message.recv().await else {
                                return Ok(());
                            };
                            self.metrics.network += start.elapsed();
                            match message {
                                Message::PushShard(push_shard) => {
                                    debug!(
                                        "Received push shard {} for round {} size {}",
                                        push_shard.shard,
                                        push_shard.round,
                                        push_shard.data.len()
                                    );
                                    if push_shard.round != self.current_round
                                        || push_shard.shard < shard
                                        || shards.contains_key(&push_shard.shard)
                                    {
                                        continue;
                                    }

                                    let start = Instant::now();

                                    let mut leaves = Vec::new();
                                    for (key, value) in &push_shard.data {
                                        let mut context = digest::Context::new(&digest::SHA256);
                                        context.update(key);
                                        context.update(value);
                                        context.update(&(self.current_round - 1).to_le_bytes());
                                        let hash = context.finish();
                                        leaves.push(hash.as_ref().try_into().unwrap());
                                    }

                                    if MerkleTree::new(leaves).root()
                                        != self.merkle_roots[push_shard.shard as usize]
                                    {
                                        anyhow::bail!(
                                            "Received shard {} with invalid merkle root",
                                            shard
                                        );
                                    }
                                    self.metrics.verify += start.elapsed();

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

                    while join_set.len() >= available_parallelism()?.get() - 1 {
                        let start = Instant::now();
                        join_set.join_next().await.unwrap()??;
                        self.metrics.update += start.elapsed();
                    }
                    self.update(&tx_stopped, &mut join_set, &mut shard_updates, shard, data);
                }
            }
            let start = Instant::now();
            while let Some(res) = join_set.join_next().await {
                res??
            }
            self.metrics.update += start.elapsed();

            self.metrics.round += round_start.elapsed();
            cancel.cancel();
        }
    }

    fn update(
        &mut self,
        tx_stopped: &Sender<()>,
        join_set: &mut JoinSet<Result<(), anyhow::Error>>,
        shard_updates: &mut Vec<HashMap<Vec<u8>, Option<Vec<u8>>>>,
        shard: u32,
        data: Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let db = self.db.clone();
        let updates = take(&mut shard_updates[shard as usize]);
        let current_round = self.current_round;
        let num_faulty_nodes = self.storage_config.num_faulty_nodes;
        let storing = self.storing_shards.contains(&shard);
        let node_index = self.node_index;
        let tx_stopped = tx_stopped.clone();
        join_set.spawn_blocking(move || {
            let _tx_stopped = tx_stopped;
            // let start = Instant::now();
            let mut data = HashMap::<_, _>::from_iter(data);
            for (key, value) in updates {
                if let Some(value) = value {
                    data.insert(key, value);
                } else {
                    data.remove(&key);
                }
            }
            let mut data = Vec::from_iter(data);
            data.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));
            // self.metrics.update += start.elapsed();

            // let start = Instant::now();
            let mut leaves = Vec::new();
            for (key, value) in &data {
                let mut context = digest::Context::new(&digest::SHA256);
                context.update(key);
                context.update(value);
                context.update(&current_round.to_le_bytes());
                let hash = context.finish();
                leaves.push(hash.as_ref().try_into().unwrap());
            }
            let tree = MerkleTree::new(leaves);
            // self.merkle_roots[shard as usize] = tree.root();
            // self.metrics.merklize += start.elapsed();

            // let start = Instant::now();
            let mut stripe_data = bincode::encode_to_vec(&data, bincode::config::standard())?;

            // let current_round = self.current_round;
            let k = (num_faulty_nodes + 1) as usize;
            let recovery_count = num_faulty_nodes * 2;
            // let tx_stopped = tx_stopped.clone();
            // let chunks = spawn_blocking(move || {
            // let _tx_stopped = tx_stopped;
            let stripe_data_len = stripe_data.len().next_multiple_of(k * 2);
            info!(
                "Archiving round {}, shard {}, data length {} -> {}",
                current_round,
                shard,
                stripe_data.len(),
                stripe_data_len
            );
            stripe_data.resize(stripe_data_len, 0);

            let mut chunks = stripe_data
                .chunks_exact(stripe_data_len / k)
                .map(|c| c.to_vec())
                .collect::<Vec<_>>();
            let encoded_chunks = reed_solomon_simd::encode(k, recovery_count as _, &chunks)?;
            info!(
                "Archived round {}, shard {}, encoded to {} chunks",
                current_round,
                shard,
                encoded_chunks.len()
            );
            chunks.extend(encoded_chunks);
            // anyhow::Ok(chunks)
            // })
            // .await??;
            // self.metrics.encode += start.elapsed();

            // let start = Instant::now();
            // TODO do not directly overwrite last round
            let Some(cf) = db.cf_handle("archive") else {
                anyhow::bail!("archive column family not found");
            };
            db.put_cf(
                cf,
                format!("{shard}.{}", node_index),
                &chunks[node_index as usize],
            )?;
            info!(
                "Archived round {}, shard {}, data size {}",
                current_round,
                shard,
                chunks[node_index as usize].len()
            );

            if storing {
                let mut write_batch = WriteBatch::new();
                for (i, (key, mut value)) in data.into_iter().enumerate() {
                    value.extend_from_slice(&(i as u32).to_le_bytes());
                    write_batch.put(&key, &value);
                }
                db.write(write_batch)?;
                let merkle_cf = db.cf_handle("merkle").unwrap();
                let tree_bytes = bincode::encode_to_vec(&tree, bincode::config::standard())?;
                db.put_cf(merkle_cf, shard.to_be_bytes(), &tree_bytes[..])?;
            }
            // self.metrics.store += start.elapsed();
            anyhow::Ok(())
        });
    }

    fn scan(
        &mut self,
        iter: &mut rocksdb::DBRawIterator<'_>,
        shards: &mut HashMap<u32, Vec<(Vec<u8>, Vec<u8>)>>,
        shard: u32,
    ) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        let prefix = shard.to_be_bytes();
        iter.seek(prefix);
        iter.status()?;
        let mut data = Vec::new();
        let mut expected_index = 0;
        while let Some((key, value)) = iter.item() {
            let Some(key) = key.strip_prefix(&prefix[..]) else {
                break;
            };
            let mut value = value.to_vec();
            let index = u32::from_le_bytes(value.split_off(value.len() - 4).try_into().unwrap());
            assert_eq!(index, expected_index);
            expected_index += 1;
            data.push((key.to_vec(), value));
            iter.next();
            iter.status()?;
        }
        self.metrics.scan += start.elapsed();
        if self.pushing_shards.contains(&shard) {
            let push_shard = message::PushShard {
                round: self.current_round,
                shard,
                data: data.clone(),
            };
            self.network_interconnect
                .send_to_all(Message::PushShard(push_shard));
        }
        shards.insert(shard, data);
        Ok(())
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
