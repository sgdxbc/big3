use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use big_schema::NodeIndex;
use log::{debug, info};
use rocksdb::{DB, WriteOptions};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::{
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel},
    task::{spawn_blocking, yield_now},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    metrics::Latency,
    network::interconnect::{NetworkInterconnectHandle, ReceiveHandle},
    storage::BigStorageConfig,
};

pub use message::Message;

pub struct ArchiveChannels {
    tx_message: UnboundedSender<Message>,
    rx_message: UnboundedReceiver<Message>,
}

impl Default for ArchiveChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl ArchiveChannels {
    pub fn new() -> Self {
        let (tx_message, rx_message) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx_message,
            rx_message,
        }
    }

    pub fn receive_handle(&self) -> ReceiveHandle<Message> {
        ReceiveHandle::new(self.tx_message.clone())
    }
}

pub struct ArchiveConfig {
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

    config: ArchiveConfig,
    storage_config: BigStorageConfig,
    storing_shards: HashSet<u32>,
    pushing_shards: HashSet<u32>,
    db: Arc<DB>,
    node_index: NodeIndex,

    current_round: u64,
    node_rounds: HashMap<NodeIndex, u64>,
    // round -> stripe -> shard -> data
    reorder_push_shards: HashMap<u64, HashMap<u32, HashMap<u32, Vec<(Vec<u8>, Vec<u8>)>>>>,

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
            current_round: 0,
            node_rounds: Default::default(),
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
        loop {
            info!("Voting to start archive for round {}", self.current_round);
            let vote = message::Vote {
                round: self.current_round,
                node_index: self.node_index,
            };
            // TODO send vote through consensus
            self.network_interconnect.send_to_all(Message::Vote(vote));
            self.node_rounds.insert(self.node_index, self.current_round);
            let mut round = self.current_round;
            while self.node_rounds.values().filter(|&&r| r == round).count()
                < (self.storage_config.num_nodes - self.storage_config.num_faulty_nodes) as usize
            {
                let Some(message) = self.channels.rx_message.recv().await else {
                    return Ok(());
                };
                match message {
                    Message::Vote(vote) => {
                        round = vote.round;
                        let node_round = self.node_rounds.entry(vote.node_index).or_insert(round);
                        if *node_round < round {
                            *node_round = round;
                        }
                    }
                    Message::PushShard(push_shard) => {
                        if push_shard.round >= self.current_round {
                            self.reorder_push_shards
                                .entry(push_shard.round)
                                .or_default()
                                .entry(self.storage_config.stripe_of_shard(push_shard.shard))
                                .or_default()
                                .insert(push_shard.shard, push_shard.data);
                        }
                    }
                }
            }
            assert_eq!(round, self.current_round); // otherwise unimplemented yet
            self.current_round = round;
            info!("Starting archive for round {}", self.current_round);
            self.metrics.round_start = Instant::now();

            let mut iter = self.db.raw_iterator();
            for stripe in 0..self.storage_config.num_stripes {
                sleep(self.config.stripe_interval).await;
                let mut stripe_shards = HashMap::default();
                for &shard in &self.storing_shards {
                    if self.storage_config.stripe_of_shard(shard) != stripe {
                        continue;
                    }

                    let prefix = shard.to_be_bytes();
                    iter.seek(prefix);
                    iter.status()?;
                    let mut data = Vec::new();
                    while let Some((key, value)) = iter.item() {
                        let Some(key) = key.strip_prefix(&prefix[..]) else {
                            break;
                        };
                        data.push((key.to_vec(), value.to_vec()));
                        iter.next();
                        iter.status()?;
                    }

                    stripe_shards.insert(shard, data.clone());
                    debug!(
                        "Archiver round {}, stripe {} collected {:?}",
                        self.current_round,
                        stripe,
                        stripe_shards.keys().collect::<Vec<_>>()
                    );
                    if self.pushing_shards.contains(&shard) {
                        let push_shard = message::PushShard {
                            round: self.current_round,
                            shard,
                            data,
                        };
                        self.network_interconnect
                            .send_to_all(Message::PushShard(push_shard));
                    }
                    yield_now().await;
                }
                info!(
                    "Archiver round {}, stripe {} collected {} shards",
                    self.current_round,
                    stripe,
                    stripe_shards.len()
                );

                if let Some(reorder_stripe) = self
                    .reorder_push_shards
                    .get_mut(&self.current_round)
                    .and_then(|stripes| stripes.remove(&stripe))
                {
                    stripe_shards.extend(reorder_stripe);
                    debug!(
                        "Archiver round {}, stripe {} reordered shards {:?}",
                        self.current_round,
                        stripe,
                        stripe_shards.keys().collect::<Vec<_>>()
                    );
                }

                assert!(
                    stripe_shards.len() <= self.storage_config.num_shards_per_stripe() as usize
                );
                while stripe_shards.len() < self.storage_config.num_shards_per_stripe() as usize {
                    let Some(message) = self.channels.rx_message.recv().await else {
                        return Ok(());
                    };
                    match message {
                        Message::PushShard(push_shard) => {
                            assert!(push_shard.round >= self.current_round);
                            let stripe_of_shard =
                                self.storage_config.stripe_of_shard(push_shard.shard);
                            if push_shard.round > self.current_round || stripe_of_shard > stripe {
                                self.reorder_push_shards
                                    .entry(push_shard.round)
                                    .or_default()
                                    .entry(stripe_of_shard)
                                    .or_default()
                                    .insert(push_shard.shard, push_shard.data);
                                continue;
                            }
                            if stripe_of_shard == stripe {
                                stripe_shards.insert(push_shard.shard, push_shard.data);
                                debug!(
                                    "Archiver round {}, stripe {} late reordered shard {:?}",
                                    self.current_round,
                                    stripe,
                                    stripe_shards.keys().collect::<Vec<_>>()
                                );
                            }
                        }
                        Message::Vote(vote) => {
                            let round = vote.round;
                            let node_round =
                                self.node_rounds.entry(vote.node_index).or_insert(round);
                            if *node_round < round {
                                *node_round = round;
                            }
                        }
                    }
                }
                info!(
                    "Archiver round {}, stripe {} finalized {} shards",
                    self.current_round,
                    stripe,
                    stripe_shards.len()
                );

                // let mut shard = 0;
                // for stripe in 0..self.storage_config.num_stripes {
                //     sleep(self.config.stripe_interval).await;
                //     let mut stripe_shards = HashMap::default();
                //     for _ in 0..self.storage_config.num_shards_per_stripe() {
                //         if self.storing_shards.contains(&shard) {
                //             let prefix = shard.to_be_bytes();
                //             iter.seek(prefix);
                //             iter.status()?;
                //             let mut data = Vec::new();
                //             while let Some((key, value)) = iter.item() {
                //                 let Some(key) = key.strip_prefix(&prefix[..]) else {
                //                     break;
                //                 };
                //                 data.push((key.to_vec(), value.to_vec()));
                //                 iter.next();
                //                 iter.status()?;
                //             }

                //             stripe_shards.insert(shard, data.clone());
                //             if self.pushing_shards.contains(&shard) {
                //                 let push_shard = message::PushShard {
                //                     round: self.current_round,
                //                     shard,
                //                     data,
                //                 };
                //                 self.network_interconnect
                //                     .send_to_all(Message::PushShard(push_shard));
                //             }
                //             yield_now().await;
                //         } else if let Some(reorder_shard) = self
                //             .reorder_push_shards
                //             .get_mut(&self.current_round)
                //             .and_then(|shards| shards.remove(&shard))
                //         {
                //             stripe_shards.insert(shard, reorder_shard);
                //         } else {
                //             loop {
                //                 let Some(message) = self.channels.rx_message.recv().await else {
                //                     return Ok(());
                //                 };
                //                 match message {
                //                     Message::PushShard(push_shard) => {
                //                         assert!(push_shard.round >= self.current_round);
                //                         if push_shard.round > self.current_round
                //                             || push_shard.shard > shard
                //                         {
                //                             self.reorder_push_shards
                //                                 .entry(push_shard.round)
                //                                 .or_default()
                //                                 .insert(push_shard.shard, push_shard.data);
                //                             continue;
                //                         }
                //                         if push_shard.shard == shard {
                //                             stripe_shards.insert(push_shard.shard, push_shard.data);
                //                             break;
                //                         }
                //                     }
                //                     Message::Vote(vote) => {
                //                         let round = vote.round;
                //                         let node_round = self
                //                             .node_rounds
                //                             .entry(vote.node_index)
                //                             .or_insert(round);
                //                         if *node_round < round {
                //                             *node_round = round;
                //                         }
                //                     }
                //                 }
                //             }
                //         }
                //         info!(
                //             "Archive round {}, stripe {} shard {}",
                //             self.current_round, stripe, shard
                //         );

                //         shard += 1;
                //     }
                // }

                let current_round = self.current_round;
                let k = (self.storage_config.num_faulty_nodes + 1) as usize;
                let db = self.db.clone();
                let recovery_count = self.storage_config.num_faulty_nodes * 2;
                let node_index = self.node_index;
                let tx_stopped = tx_stopped.clone();
                spawn_blocking(move || {
                    let _tx_stopped = tx_stopped;
                    let mut stripe_data = stripe_shards
                        .into_iter()
                        .flat_map(|(_, data)| data)
                        .collect::<Vec<_>>();
                    stripe_data.sort_unstable_by(|(k1, _), (k2, _)| k1.cmp(k2));
                    let mut stripe_data =
                        bincode::encode_to_vec(stripe_data, bincode::config::standard())?;
                    let stripe_data_len = stripe_data.len().next_multiple_of(k * 2);
                    info!(
                        "Archiving round {}, stripe {}, data length {} -> {}",
                        current_round,
                        stripe,
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
                        "Archived round {}, stripe {}, encoded to {} chunks",
                        current_round,
                        stripe,
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
                    let mut write_opts = WriteOptions::default();
                    write_opts.set_low_pri(true);
                    let Some(cf) = db.cf_handle("archive") else {
                        anyhow::bail!("archive column family not found");
                    };

                    db.put_cf_opt(cf, format!("{stripe}.{}", node_index), value, &write_opts)?;
                    info!(
                        "Archived round {}, stripe {}, data size {}",
                        current_round,
                        stripe,
                        value.len()
                    );
                    anyhow::Ok(())
                })
                .await??;
            }

            self.metrics.round += self.metrics.round_start.elapsed();
            self.current_round += 1;
        }
    }
}

mod message {
    use big_schema::NodeIndex;
    use bincode::{Decode, Encode};

    #[derive(Decode, Encode)]
    pub enum Message {
        PushShard(PushShard),
        Vote(Vote),
    }

    #[derive(Decode, Encode)]
    pub struct PushShard {
        pub round: u64,
        pub shard: u32,
        pub data: Vec<(Vec<u8>, Vec<u8>)>,
    }

    #[derive(Decode, Encode)]
    pub struct Vote {
        pub round: u64,
        pub node_index: NodeIndex,
    }
}
