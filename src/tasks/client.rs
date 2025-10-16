use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use quinn::{Connection, Endpoint};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::client_config,
    client::{Client, ClientContext, ClientWorker, ClientWorkerContext, InvokeId, Records},
    schema,
    types::{ClientId, NodeIndex, Reply, Request},
};

pub struct ClientWorkerTask {
    client_worker: ClientWorker<ClientWorkerTaskContext>,
}

impl ClientWorkerTask {
    fn new(client_worker: ClientWorker<ClientWorkerTaskContext>) -> Self {
        Self { client_worker }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        self.client_worker.start();
        while let Some((seq, res)) = self.client_worker.context.rx_invoke_response.recv().await {
            self.client_worker.on_invoke_response(seq, res);
        }
    }
}

struct ClientWorkerTaskContext {
    tx_invoke: UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    tx_invoke_response: Sender<(InvokeId, Vec<u8>)>,
    rx_invoke_response: Receiver<(InvokeId, Vec<u8>)>,
    invoke_id: InvokeId,
}

impl ClientWorkerTaskContext {
    fn new(tx_invoke: UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>) -> Self {
        let (tx_invoke_response, rx_invoke_response) = channel(100);
        Self {
            tx_invoke,
            tx_invoke_response,
            rx_invoke_response,
            invoke_id: 0,
        }
    }
}

impl ClientWorkerContext for ClientWorkerTaskContext {
    fn invoke(&mut self, command: Vec<u8>) -> InvokeId {
        let (tx_response, rx_response) = oneshot::channel();
        self.invoke_id += 1;
        let seq = self.invoke_id;
        let _ = self.tx_invoke.send((command, tx_response));
        let tx_finalize = self.tx_invoke_response.clone();
        tokio::spawn(async move {
            let _ = tx_finalize.send((seq, rx_response.await?)).await;
            anyhow::Ok(())
        });
        seq
    }
}

struct ClientChannels {
    tx_invoke: UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    rx_invoke: UnboundedReceiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,

    tx_incoming_message: Sender<Reply>,
    rx_incoming_message: Receiver<Reply>,
}

impl ClientChannels {
    fn new() -> Self {
        let (tx_invoke, rx_invoke) = unbounded_channel();
        let (tx_incoming_message, rx_incoming_message) = channel(100);
        Self {
            tx_invoke,
            rx_invoke,
            tx_incoming_message,
            rx_incoming_message,
        }
    }
}

pub struct ClientTask {
    channels: ClientChannels,
    client: Client<ClientTaskContext>,
}

impl ClientTask {
    fn new(channels: ClientChannels, client: Client<ClientTaskContext>) -> Self {
        Self { channels, client }
    }

    async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((command, tx_response)) = self.channels.rx_invoke.recv() => {
                    self.client.invoke(command, tx_response);
                }
                Some(reply) = self.channels.rx_incoming_message.recv() => {
                    self.client.on_message(reply);
                }
            }
        }
    }
}

struct ClientTaskContext {
    tx_outgoing_message: UnboundedSender<(NodeIndex, Request)>,
}

impl ClientContext for ClientTaskContext {
    fn send(&mut self, to: NodeIndex, request: Request) {
        let _ = self.tx_outgoing_message.send((to, request));
    }
}

pub struct NetworkOutgoingTask {
    tx_outgoing_message: UnboundedSender<(NodeIndex, Request)>,
    rx_outgoing_message: UnboundedReceiver<(NodeIndex, Request)>,

    connections: HashMap<NodeIndex, Connection>,
}

impl NetworkOutgoingTask {
    fn new(connections: HashMap<NodeIndex, Connection>) -> Self {
        let (tx_outgoing_message, rx_outgoing_message) = unbounded_channel();
        Self {
            tx_outgoing_message,
            rx_outgoing_message,
            connections,
        }
    }

    pub async fn load(
        client_id: ClientId,
        addrs: Vec<SocketAddr>,
        tx_incoming_message: Sender<Reply>,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
        endpoint.set_default_client_config(client_config());
        let mut connections = HashMap::new();
        for (i, &addr) in addrs.iter().enumerate() {
            let conn = endpoint.connect(addr, "server.example")?.await?;
            conn.open_uni()
                .await?
                .write_all(&client_id.to_le_bytes())
                .await?;
            tokio::spawn(Self::handle_connection(
                conn.clone(),
                tx_incoming_message.clone(),
            ));
            connections.insert(i as _, conn);
        }
        Ok(Self::new(connections))
    }

    async fn handle_connection(
        conn: Connection,
        tx_incoming_message: Sender<Reply>,
    ) -> anyhow::Result<()> {
        loop {
            let mut recv = conn.accept_uni().await?;
            // let tx_incoming_message = tx_incoming_message.clone();
            // tokio::spawn(async move {
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = tx_incoming_message.send(message).await;
            // anyhow::Ok(())
            // });
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some((to, message)) = self.rx_outgoing_message.recv().await {
            self.handle_outgoing_message(to, message).await
        }
    }

    // fn handle_outgoing_message(&mut self, id: NodeIndex, request: Request) {
    //     if let Some(conn) = self.connections.get(&id) {
    //         let conn = conn.clone();
    //         tokio::spawn(async move {
    //             let mut send = conn.open_uni().await?;
    //             let bytes = bincode::encode_to_vec(&request, bincode::config::standard())?;
    //             send.write_all(&bytes).await?;
    //             anyhow::Ok(())
    //         });
    //     }
    // }

    async fn handle_outgoing_message(&mut self, id: NodeIndex, request: Request) {
        if let Some(conn) = self.connections.get(&id) {
            // let conn = conn.clone();
            // tokio::spawn(async move {
            let _ = async {
                let mut send = conn.open_uni().await?;
                let bytes = bincode::encode_to_vec(&request, bincode::config::standard())?;
                send.write_all(&bytes).await?;
                anyhow::Ok(())
                // });
            }
            .await;
        }
    }
}

pub struct ClientNodeTask {
    network_outgoing: NetworkOutgoingTask,
    client_worker: ClientWorkerTask,
    client: ClientTask,
}

impl ClientNodeTask {
    pub async fn load(schema: schema::ClientTask) -> anyhow::Result<Self> {
        let client_channels = ClientChannels::new();
        let client_id = rand::random();

        let network_outgoing = NetworkOutgoingTask::load(
            client_id,
            schema.addrs,
            client_channels.tx_incoming_message.clone(),
        )
        .await?;

        let client_context = ClientTaskContext {
            tx_outgoing_message: network_outgoing.tx_outgoing_message.clone(),
        };
        let client = ClientTask::new(
            client_channels,
            Client::new(client_context, schema.config, client_id),
        );

        let client_worker_context = ClientWorkerTaskContext::new(client.channels.tx_invoke.clone());
        let client_worker = ClientWorkerTask::new(ClientWorker::new(
            client_worker_context,
            schema.worker_config,
        ));
        Ok(Self {
            network_outgoing,
            client,
            client_worker,
        })
    }

    pub fn scrape_state(&self) -> Arc<Mutex<Records>> {
        Arc::clone(&self.client_worker.client_worker.records)
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.client.run(stop.clone()),
            self.client_worker.run(stop.clone())
        )?;
        Ok(())
    }
}
