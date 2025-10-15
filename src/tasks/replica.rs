use std::collections::HashMap;

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
    execute::{Execute, ExecuteContext},
    types::{ClientId, Reply, Request},
};

pub struct ExecuteTask {
    tx_incoming_message: Sender<Request>,
    rx_incoming_message: Receiver<Request>,
    execute: Execute<Context>,
}

impl ExecuteTask {
    fn new(execute: Execute<Context>) -> Self {
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        Self {
            tx_incoming_message,
            rx_incoming_message,
            execute,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some(request) = self.rx_incoming_message.recv().await {
            self.execute.on_request(request);
        }
    }
}

struct Context {
    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
}

impl ExecuteContext for Context {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.tx_outgoing_message.send((id, reply));
    }
}

pub struct NetworkIncomingTask {
    endpoint: Endpoint,

    // execute handle
    tx_incoming_message: Sender<Request>,

    // outgoing handle
    tx_connection: Sender<(ClientId, Connection)>,
}

impl NetworkIncomingTask {
    pub fn new(
        endpoint: Endpoint,
        tx_incoming_message: Sender<Request>,
        tx_connection: Sender<(ClientId, Connection)>,
    ) -> Self {
        Self {
            endpoint,
            tx_incoming_message,
            tx_connection,
        }
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
            let _ = self
                .tx_connection
                .send((ClientId::from_le_bytes(client_id), conn.clone()))
                .await;
            tokio::spawn(Self::handle_connection(
                conn,
                self.tx_incoming_message.clone(),
            ));
        }
        Ok(())
    }

    async fn handle_connection(
        conn: Connection,
        tx_incoming_message: Sender<Request>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            let tx_incoming_message = tx_incoming_message.clone();
            tokio::spawn(async move {
                let bytes = recv.read_to_end(usize::MAX).await?;
                let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
                let _ = tx_incoming_message.send(message).await;
                anyhow::Ok(())
            });
        }
    }
}

pub struct NetworkOutgoingTask {
    tx_connection: Sender<(ClientId, Connection)>,
    rx_connection: Receiver<(ClientId, Connection)>,

    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    rx_outgoing_message: UnboundedReceiver<(ClientId, Reply)>,

    connections: HashMap<ClientId, Connection>,
}

impl NetworkOutgoingTask {
    fn new() -> Self {
        let (tx_connection, rx_connection) = channel(100);
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_connection,
            rx_connection,
            tx_outgoing_message,
            rx_outgoing_message,
            connections: Default::default(),
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await })
            .await?
            .unwrap_or(Ok(()))?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Some((id, conn)) = self.rx_connection.recv() => {
                    self.handle_connection(id, conn);
                }
                Some((id, reply)) = self.rx_outgoing_message.recv() => {
                    self.handle_outgoing_message(id, reply);
                }
            }
        }
    }

    fn handle_connection(&mut self, id: ClientId, conn: Connection) {
        self.connections.insert(id, conn.clone());
    }

    fn handle_outgoing_message(&mut self, id: ClientId, reply: Reply) {
        if let Some(conn) = self.connections.get(&id) {
            let conn = conn.clone();
            tokio::spawn(async move {
                let mut send = conn.open_uni().await?;
                let bytes = bincode::encode_to_vec(&reply, bincode::config::standard())?;
                send.write_all(&bytes).await?;
                anyhow::Ok(())
            });
        }
    }
}

pub struct ReplicaNodeTask {
    network_outgoing: NetworkOutgoingTask,
    execute: ExecuteTask,
    network_incoming: NetworkIncomingTask,
}

impl ReplicaNodeTask {
    pub async fn load() -> anyhow::Result<Self> {
        let network_outgoing = NetworkOutgoingTask::new();
        let execute_context = Context {
            tx_outgoing_message: network_outgoing.tx_outgoing_message.clone(),
        };
        let execute = ExecuteTask::new(Execute::new(execute_context, 0));
        let endpoint = Endpoint::server(server_config(), ([0, 0, 0, 0], 5000).into())?;
        let network_incoming = NetworkIncomingTask::new(
            endpoint,
            execute.tx_incoming_message.clone(),
            network_outgoing.tx_connection.clone(),
        );
        Ok(Self {
            network_outgoing,
            execute,
            network_incoming,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.network_incoming.run(stop.clone()),
        )?;
        Ok(())
    }
}
