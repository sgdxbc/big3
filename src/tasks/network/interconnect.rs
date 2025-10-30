use std::{collections::HashMap, time::Duration};

use bincode::{Decode, Encode};
use bytes::Bytes;
use log::error;
use quinn::{Connection, Endpoint, TransportConfig};
use tokio::{
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::{client_config, server_config},
    schema,
    types::NodeIndex,
};

pub struct ReceiveHandle<M> {
    tx_incoming_message: Sender<M>,
}

impl<M> Clone for ReceiveHandle<M> {
    fn clone(&self) -> Self {
        Self {
            tx_incoming_message: self.tx_incoming_message.clone(),
        }
    }
}

impl<M> ReceiveHandle<M> {
    pub fn new(tx_incoming_message: Sender<M>) -> Self {
        Self {
            tx_incoming_message,
        }
    }

    pub async fn incoming_message(&self, message: M) -> anyhow::Result<()>
    where
        M: Send + Sync + 'static,
    {
        self.tx_incoming_message.send(message).await?;
        Ok(())
    }
}

pub struct NetworkInterconnectTask {
    txs_outgoing_message: HashMap<NodeIndex, UnboundedSender<Bytes>>,
    join_set: JoinSet<anyhow::Result<()>>,
}

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

impl NetworkInterconnectTask {
    pub async fn load<M: Decode<()> + Send + Sync + 'static>(
        receive: ReceiveHandle<M>,
        schema: &schema::ReplicaTask,
        port: u16,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::server(
            server_config(),
            (schema.ips[schema.node_index as usize], port).into(),
        )?;
        let mut transport_config = TransportConfig::default();
        transport_config.keep_alive_interval(Duration::from_secs(10).into());
        let mut config = client_config();
        config.transport_config(transport_config.into());
        endpoint.set_default_client_config(config);

        let connect = async {
            let mut txs = HashMap::new();
            for (i, &ip) in schema.ips[..schema.node_index as usize].iter().enumerate() {
                let conn = endpoint
                    .connect((ip, port).into(), "server.example")?
                    .await?;
                conn.open_uni()
                    .await?
                    .write_all(&schema.node_index.to_le_bytes())
                    .await?;
                txs.insert(i as NodeIndex, conn);
            }
            anyhow::Ok(txs)
        };
        let accept = async {
            let mut txs = HashMap::new();
            while txs.len() < (schema.ips.len() - schema.node_index as usize - 1) {
                let conn = endpoint.accept().await.unwrap().await?;
                let mut client_id = [0; size_of::<NodeIndex>()];
                conn.accept_uni().await?.read_exact(&mut client_id).await?;
                let client_index = NodeIndex::from_le_bytes(client_id);
                txs.insert(client_index, conn);
            }
            anyhow::Ok(txs)
        };
        let (txs_lower, txs_higher) = tokio::try_join!(connect, accept)?;

        let mut txs_outgoing_message = HashMap::new();
        let mut join_set = JoinSet::new();
        for (node_index, conn) in txs_lower.into_iter().chain(txs_higher) {
            join_set.spawn(Self::run_connection_incoming(conn.clone(), receive.clone()));
            let (tx_outgoing, rx_outgoing) = unbounded_channel();
            join_set.spawn(Self::run_connection_outgoing(conn, rx_outgoing));
            txs_outgoing_message.insert(node_index, tx_outgoing);
        }
        Ok(Self {
            txs_outgoing_message,
            join_set,
        })
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
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = receive.incoming_message(message).await;
        }
    }

    async fn run_connection_outgoing(
        conn: Connection,
        mut tx_outgoing_message: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        while let Some(bytes) = tx_outgoing_message.recv().await {
            let mut send = conn.open_uni().await?;
            send.write_all(&bytes).await?;
        }
        Ok(())
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))
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
