use std::{collections::HashMap, time::Instant};

use bytes::Bytes;
use quinn::{Connection, Endpoint};
use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::server_config,
    types::{ClientId, Reply, Request},
};

#[derive(Clone)]
pub struct SubmitHandle {
    tx_request: Sender<(Instant, Request)>,
}

impl SubmitHandle {
    pub fn new(tx_request: Sender<(Instant, Request)>) -> Self {
        Self { tx_request }
    }

    pub async fn submit(&self, request: Request) -> anyhow::Result<()> {
        self.tx_request.send((Instant::now(), request)).await?;
        Ok(())
    }
}

pub struct NetworkAcceptTask {
    endpoint: Endpoint,
    submit: SubmitHandle,
    network_outgoing: NetworkOutgoingHandle,
}

impl NetworkAcceptTask {
    fn new(
        endpoint: Endpoint,
        submit: SubmitHandle,
        network_outgoing: NetworkOutgoingHandle,
    ) -> Self {
        Self {
            endpoint,
            submit,
            network_outgoing,
        }
    }

    pub async fn load(
        submit: SubmitHandle,
        network_outgoing: NetworkOutgoingHandle,
    ) -> anyhow::Result<Self> {
        let endpoint = Endpoint::server(server_config(), ([0, 0, 0, 0], 5000).into())?;
        Ok(Self::new(endpoint, submit, network_outgoing))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(incoming) = self.endpoint.accept().await {
            let conn = incoming.await?;
            let mut client_id = [0; size_of::<ClientId>()];
            conn.accept_uni().await?.read_exact(&mut client_id).await?;

            let (tx_outgoing, rx_outgoing) = unbounded_channel();
            let _ = self
                .network_outgoing
                .new_connection(ClientId::from_le_bytes(client_id), tx_outgoing)
                .await;
            tokio::spawn(Self::run_connection_incoming(
                conn.clone(),
                self.submit.clone(),
            ));
            tokio::spawn(Self::run_connection_outgoing(conn, rx_outgoing));
        }
        Ok(())
    }

    async fn run_connection_incoming(conn: Connection, submit: SubmitHandle) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = submit.submit(message).await;
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
}

pub struct NetworkOutgoingChannels {
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    rx_connection: Receiver<(ClientId, UnboundedSender<Bytes>)>,

    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    rx_outgoing_message: UnboundedReceiver<(ClientId, Reply)>,
}

pub struct NetworkOutgoingHandle {
    tx_connection: Sender<(ClientId, UnboundedSender<Bytes>)>,
    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
}

impl Default for NetworkOutgoingChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkOutgoingChannels {
    pub fn new() -> Self {
        let (tx_connection, rx_connection) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_connection,
            rx_connection,
            tx_outgoing_message,
            rx_outgoing_message,
        }
    }

    pub fn handle(&self) -> NetworkOutgoingHandle {
        NetworkOutgoingHandle {
            tx_connection: self.tx_connection.clone(),
            tx_outgoing_message: self.tx_outgoing_message.clone(),
        }
    }
}

impl NetworkOutgoingHandle {
    async fn new_connection(
        &self,
        id: ClientId,
        conn: UnboundedSender<Bytes>,
    ) -> anyhow::Result<()> {
        self.tx_connection.send((id, conn)).await?;
        anyhow::Ok(())
    }

    pub fn send_message(&self, id: ClientId, reply: Reply) -> anyhow::Result<()> {
        self.tx_outgoing_message.send((id, reply))?;
        anyhow::Ok(())
    }
}

pub struct NetworkOutgoingTask {
    pub channels: NetworkOutgoingChannels,
    connections: HashMap<ClientId, UnboundedSender<Bytes>>,
}

impl NetworkOutgoingTask {
    fn new(channels: NetworkOutgoingChannels) -> Self {
        Self {
            channels,
            connections: Default::default(),
        }
    }

    pub async fn load(channels: NetworkOutgoingChannels) -> anyhow::Result<Self> {
        Ok(Self::new(channels))
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Some((id, conn)) = self.channels.rx_connection.recv() => {
                    self.handle_connection(id, conn);
                }
                Some((id, reply)) = self.channels.rx_outgoing_message.recv() => {
                    self.handle_outgoing_message(id, reply)?;
                }
            }
        }
    }

    fn handle_connection(&mut self, id: ClientId, conn: UnboundedSender<Bytes>) {
        self.connections.insert(id, conn);
    }

    fn handle_outgoing_message(&mut self, id: ClientId, reply: Reply) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(&reply, bincode::config::standard())?;
        let _ = self.connections[&id].send(bytes.into());
        anyhow::Ok(())
    }
}
