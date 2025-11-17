use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use quinn::{Connection, Endpoint};
use tokio::{
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
    time::interval,
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::server_config,
    common::{ClientId, Reply, Request},
};

#[derive(Clone)]
pub struct SubmitHandle {
    tx_request: Sender<Request>,
}

impl SubmitHandle {
    pub fn new(tx_request: Sender<Request>) -> Self {
        Self { tx_request }
    }

    pub async fn submit(&self, request: Request) -> anyhow::Result<()> {
        self.tx_request.send(request).await?;
        Ok(())
    }
}

pub struct NetworkAcceptTask<const BATCH: bool = false> {
    endpoint: Endpoint,
    submit: SubmitHandle,
    network_outgoing: NetworkOutgoingHandle,
}

impl<const BATCH: bool> NetworkAcceptTask<BATCH> {
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
            let submit = self.submit.clone();
            let network_outgoing = self.network_outgoing.clone();
            let conn = incoming.await?;
            tokio::spawn(async move {
                let mut client_id = [0; size_of::<ClientId>()];
                conn.accept_uni().await?.read_exact(&mut client_id).await?;

                let (tx_outgoing, rx_outgoing) = unbounded_channel();
                tokio::spawn(Self::run_connection_incoming(conn.clone(), submit.clone()));
                tokio::spawn(Self::run_connection_outgoing(conn, rx_outgoing));
                let _ = network_outgoing
                    .new_connection(ClientId::from_le_bytes(client_id), tx_outgoing)
                    .await;
                anyhow::Ok(())
            });
        }
        Ok(())
    }

    async fn run_connection_incoming(conn: Connection, submit: SubmitHandle) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let mut bytes = &*recv.read_to_end(usize::MAX).await?;
            while !bytes.is_empty() {
                let (message, len) =
                    bincode::decode_from_slice(bytes, bincode::config::standard())?;
                let _ = submit.submit(message).await;
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
            rx_outgoing_message.recv_many(&mut buf, 1_000).await > 0
        } {
            let mut send = conn.open_uni().await?;
            send.write_all_chunks(&mut buf).await?;
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

#[derive(Clone)]
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
