use std::{collections::HashMap, time::Duration};

use bincode::{Decode, Encode};
use bytes::{Buf as _, Bytes};
use log::error;
use quinn::{Connection, Endpoint, TransportConfig};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinSet,
    time::{Instant, interval, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::{client_config, server_config},
    common::NodeIndex,
    schema,
};

pub struct ReceiveHandle<M> {
    tx_incoming_message: UnboundedSender<M>,
}

impl<M> Clone for ReceiveHandle<M> {
    fn clone(&self) -> Self {
        Self {
            tx_incoming_message: self.tx_incoming_message.clone(),
        }
    }
}

impl<M> ReceiveHandle<M> {
    pub fn new(tx_incoming_message: UnboundedSender<M>) -> Self {
        Self {
            tx_incoming_message,
        }
    }

    pub fn incoming_message(&self, message: M) -> anyhow::Result<()>
    where
        M: Send + Sync + 'static,
    {
        self.tx_incoming_message.send(message)?;
        Ok(())
    }
}

pub struct NetworkInterconnectTask<const BATCH: bool = false, const THROTTLE: bool = false> {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
    join_set: JoinSet<anyhow::Result<()>>,
    connections: Vec<Connection>,
}

#[derive(Clone)]
pub struct NetworkInterconnectHandle {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
}

impl NetworkInterconnectHandle {
    pub fn send<M: Encode>(&self, node_index: NodeIndex, message: M) {
        let bytes = bincode::encode_to_vec(&message, bincode::config::standard()).unwrap();
        let _ = self.txs_outgoing_message[&node_index].send(bytes.into());
    }

    pub fn send_to_all<M: Encode>(&self, message: M) {
        let bytes =
            Bytes::from(bincode::encode_to_vec(&message, bincode::config::standard()).unwrap());
        for tx in self.txs_outgoing_message.values() {
            let _ = tx.send(bytes.clone());
        }
    }
}

impl<const BATCH: bool, const THROTTLE: bool> NetworkInterconnectTask<BATCH, THROTTLE> {
    pub async fn load<M: Decode<()> + Send + Sync + 'static>(
        receive: ReceiveHandle<M>,
        schema: &schema::ReplicaTask,
        port: u16,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::server(
            server_config(),
            (schema.ips[schema.config.node_index as usize], port).into(),
        )?;
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Duration::from_secs(10).into());
        let mut config = client_config();
        config.transport_config(transport_config.into());
        endpoint.set_default_client_config(config);

        let connect = async {
            let mut txs = HashMap::new();
            for (i, &ip) in schema.ips[..schema.config.node_index as usize]
                .iter()
                .enumerate()
            {
                let conn = endpoint
                    .connect((ip, port).into(), "server.example")?
                    .await?;
                conn.open_uni()
                    .await?
                    .write_all(&schema.config.node_index.to_le_bytes())
                    .await?;
                txs.insert(i as NodeIndex, conn);
            }
            anyhow::Ok(txs)
        };
        let accept = async {
            let mut txs = HashMap::new();
            while txs.len() < (schema.ips.len() - schema.config.node_index as usize - 1) {
                let conn = endpoint.accept().await.unwrap().await?;
                let mut client_id = [0; size_of::<NodeIndex>()];
                conn.accept_uni().await?.read_exact(&mut client_id).await?;
                let node_index = NodeIndex::from_le_bytes(client_id);
                txs.insert(node_index, conn);
            }
            anyhow::Ok(txs)
        };
        let (txs_lower, txs_higher) = tokio::try_join!(connect, accept)?;

        let mut txs_outgoing_message = HashMap::new();
        let mut join_set = JoinSet::new();
        let mut connections = Vec::new();
        for (node_index, conn) in txs_lower.into_iter().chain(txs_higher) {
            connections.push(conn.clone());

            join_set.spawn(Self::run_connection_incoming(conn.clone(), receive.clone()));
            let (tx_outgoing, rx_outgoing) = unbounded_channel();
            if let Some(latencies) = &schema.latencies {
                let latency =
                    latencies[schema.config.node_index as usize][node_index as usize].max(1);
                assert!(latency < 500);
                join_set.spawn(Self::run_connection_outgoing_with_latency(
                    conn,
                    rx_outgoing,
                    latency,
                ));
            } else if THROTTLE {
                join_set.spawn(Self::run_connection_outgoing_throttled(conn, rx_outgoing));
            } else {
                join_set.spawn(Self::run_connection_outgoing(conn, rx_outgoing));
            }
            txs_outgoing_message.insert(node_index, tx_outgoing);
        }
        Ok(Self {
            txs_outgoing_message,
            join_set,
            connections,
        })
    }

    fn total_egress(&self) -> u64 {
        self.connections
            .iter()
            .map(|conn| conn.stats().udp_tx.bytes)
            .sum()
    }

    pub fn handle(&self) -> NetworkInterconnectHandle {
        NetworkInterconnectHandle {
            txs_outgoing_message: self.txs_outgoing_message.clone(),
        }
    }

    async fn run_connection_incoming<M: Decode<()> + Send + Sync + 'static>(
        conn: Connection,
        receive: ReceiveHandle<M>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let mut bytes = &*recv.read_to_end(usize::MAX).await?;
            while !bytes.is_empty() {
                let (message, len) =
                    bincode::decode_from_slice(bytes, bincode::config::standard())?;
                let _ = receive.incoming_message(message);
                bytes = &bytes[len..];
            }
        }
    }

    async fn run_connection_outgoing(
        conn: Connection,
        mut rx_outgoing_message: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        let mut buf;
        let mut interval = interval(Duration::from_millis(1));
        while {
            if BATCH {
                interval.tick().await;
            }
            buf = Vec::new();
            rx_outgoing_message.recv_many(&mut buf, usize::MAX).await > 0
        } {
            let mut send = conn.open_uni().await?;
            send.write_all_chunks(&mut buf).await?;
        }
        Ok(())
    }

    async fn run_connection_outgoing_with_latency(
        conn: Connection,
        mut rx_outgoing_message: UnboundedReceiver<Bytes>,
        latency: u32,
    ) -> anyhow::Result<()> {
        let mut buckets = vec![Vec::new(); latency as _];
        let mut bucket_index = 0;
        let mut deadline = Instant::now();
        while !rx_outgoing_message.is_closed() {
            if !buckets[bucket_index].is_empty() {
                let mut send = conn.open_uni().await?;
                send.write_all_chunks(&mut buckets[bucket_index]).await?;
            }
            buckets[bucket_index].clear();
            deadline += Duration::from_millis(1);
            let _ = timeout_at(deadline, async {
                while rx_outgoing_message
                    .recv_many(&mut buckets[bucket_index], usize::MAX)
                    .await
                    > 0
                {}
            })
            .await;
            bucket_index = (bucket_index + 1) % latency as usize;
        }
        Ok(())
    }

    async fn run_connection_outgoing_throttled(
        conn: Connection,
        mut rx_outgoing_message: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        let mut interval = interval(Duration::from_millis(4));
        while let Some(mut message) = rx_outgoing_message.recv().await {
            let mut send = conn.open_uni().await?;
            while !message.is_empty() {
                interval.tick().await;
                let chunk_size = message.len().min(1 << 10);
                send.write_all(&message[..chunk_size]).await?;
                message.advance(chunk_size);
            }
        }
        Ok(())
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<u64> {
        tokio::spawn(async move {
            stop.run_until_cancelled(self.run_inner()).await;
            Ok(self.total_egress())
        })
        .await?
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(res) = self.join_set.join_next().await {
            if let Err(err) = res.unwrap() {
                error!("network connect task error: {}", err);
            }
        }
        Ok(())
    }
}
