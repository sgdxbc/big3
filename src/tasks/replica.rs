use std::collections::HashMap;

use quinn::{Connection, Endpoint};
use tempfile::{TempDir, tempdir};
use tokio::{
    process::Command,
    select,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    cert::server_config,
    consensus::{Consensus, ConsensusContext},
    execute::{Execute, ExecuteContext, FetchId},
    storage::{Storage, StorageOp},
    tasks::PREFILL_PATH,
    types::{ClientId, Reply, Request},
};

pub struct ConsensusTask {
    tx_request: Sender<Request>,
    rx_request: Receiver<Request>,
    consensus: Consensus<ConsensusTaskContext>,
}

impl ConsensusTask {
    fn new(consensus: Consensus<ConsensusTaskContext>) -> Self {
        let (tx_request, rx_request) = channel(100);
        Self {
            tx_request,
            rx_request,
            consensus,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        while let Some(request) = self.rx_request.recv().await {
            self.consensus.on_request(request);
        }
    }
}

struct ConsensusTaskContext {
    tx_requests: UnboundedSender<Vec<Request>>,
}

impl ConsensusContext for ConsensusTaskContext {
    fn execute(&mut self, requests: Vec<Request>) {
        let _ = self.tx_requests.send(requests);
    }
}

pub struct ExecuteTask {
    tx_requests: UnboundedSender<Vec<Request>>,
    rx_requests: UnboundedReceiver<Vec<Request>>,
    execute: Execute<ExecuteTaskContext>,
}

impl ExecuteTask {
    fn new(execute: Execute<ExecuteTaskContext>) -> Self {
        let (tx_requests, rx_requests) = unbounded_channel();
        Self {
            tx_requests,
            rx_requests,
            execute,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            select! {
                Some((fetch_id, value)) = self.execute.context.rx_fetch_response.recv() => {
                    self.execute.on_fetch_response(fetch_id, value);
                }
                Some(request) = self.rx_requests.recv() => {
                    self.execute.on_requests(request);
                }
            }
        }
    }
}

struct ExecuteTaskContext {
    tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
    tx_storage_op: UnboundedSender<StorageOp>,
    tx_fetch_response: Sender<(FetchId, Option<Vec<u8>>)>,
    rx_fetch_response: Receiver<(FetchId, Option<Vec<u8>>)>,
    fetch_id: FetchId,
}

impl ExecuteTaskContext {
    fn new(
        tx_outgoing_message: UnboundedSender<(ClientId, Reply)>,
        tx_storage_op: UnboundedSender<StorageOp>,
    ) -> Self {
        let (tx_fetch_response, rx_fetch_response) = channel(100);
        Self {
            tx_outgoing_message,
            tx_storage_op,
            tx_fetch_response,
            rx_fetch_response,
            fetch_id: 0,
        }
    }
}

impl ExecuteContext for ExecuteTaskContext {
    fn send(&mut self, id: ClientId, reply: Reply) {
        let _ = self.tx_outgoing_message.send((id, reply));
    }

    fn fetch(&mut self, key: [u8; 32]) -> FetchId {
        self.fetch_id += 1;
        let fetch_id = self.fetch_id;
        let (tx_response, rx_response) = oneshot::channel();
        let _ = self.tx_storage_op.send(StorageOp::Fetch(key, tx_response));
        let tx_fetch_response = self.tx_fetch_response.clone();
        tokio::spawn(async move {
            let _ = tx_fetch_response.send((fetch_id, rx_response.await?)).await;
            anyhow::Ok(())
        });
        fetch_id
    }

    fn post(&mut self, updates: Vec<([u8; 32], Option<Vec<u8>>)>) {
        let _ = self.tx_storage_op.send(StorageOp::Post(updates));
    }
}

pub struct StorageTask {
    tx_storage_op: UnboundedSender<StorageOp>,
    rx_storage_op: UnboundedReceiver<StorageOp>,
    storage: Storage,
}

impl StorageTask {
    fn new(storage: Storage) -> Self {
        let (tx_storage_op, rx_storage_op) = unbounded_channel();
        Self {
            tx_storage_op,
            rx_storage_op,
            storage,
        }
    }

    pub async fn run(mut self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::spawn(async move { stop.run_until_cancelled(self.run_inner()).await }).await?;
        Ok(())
    }

    async fn run_inner(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_storage_op.recv().await {
            self.storage.invoke(op)?;
        }
        Ok(())
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
            // let tx_incoming_message = tx_incoming_message.clone();
            // tokio::spawn(async move {
            let bytes = recv.read_to_end(usize::MAX).await?;
            let message = bincode::decode_from_slice(&bytes, bincode::config::standard())?.0;
            let _ = tx_incoming_message.send(message).await;
            //     anyhow::Ok(())
            // });
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
                    // self.handle_outgoing_message(id, reply);
                    self.handle_outgoing_message(id, reply).await;
                }
            }
        }
    }

    fn handle_connection(&mut self, id: ClientId, conn: Connection) {
        self.connections.insert(id, conn.clone());
    }

    // fn handle_outgoing_message(&mut self, id: ClientId, reply: Reply) {
    //     if let Some(conn) = self.connections.get(&id) {
    //         let conn = conn.clone();
    //         tokio::spawn(async move {
    //             let mut send = conn.open_uni().await?;
    //             let bytes = bincode::encode_to_vec(&reply, bincode::config::standard())?;
    //             send.write_all(&bytes).await?;
    //             anyhow::Ok(())
    //         });
    //     }
    // }

    async fn handle_outgoing_message(&mut self, id: ClientId, reply: Reply) {
        if let Some(conn) = self.connections.get(&id) {
            // let conn = conn.clone();
            // tokio::spawn(async move {
            let _ = async {
                let mut send = conn.open_uni().await?;
                let bytes = bincode::encode_to_vec(&reply, bincode::config::standard())?;
                send.write_all(&bytes).await?;
                anyhow::Ok(())
                // });
            }
            .await;
        }
    }
}

pub struct ReplicaNodeTask {
    network_outgoing: NetworkOutgoingTask,
    consensus: ConsensusTask,
    execute: ExecuteTask,
    storage: StorageTask,
    network_incoming: NetworkIncomingTask,
    _temp_dir: TempDir,
}

impl ReplicaNodeTask {
    pub async fn load() -> anyhow::Result<Self> {
        let network_outgoing = NetworkOutgoingTask::new();

        let temp_dir = tempdir()?;
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PREFILL_PATH)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success(), "failed to copy prefill data");
        let storage = StorageTask::new(Storage::new(temp_dir.path())?);

        let execute_context = ExecuteTaskContext::new(
            network_outgoing.tx_outgoing_message.clone(),
            storage.tx_storage_op.clone(),
        );
        let execute = ExecuteTask::new(Execute::new(execute_context, 0));

        let consensus_context = ConsensusTaskContext {
            tx_requests: execute.tx_requests.clone(),
        };
        let consensus = ConsensusTask::new(Consensus::new(consensus_context, 0));

        let endpoint = {
            let mut server_config = server_config();
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_concurrent_uni_streams(1000u32.into());
            server_config.transport_config(transport_config.into());
            Endpoint::server(server_config, ([0, 0, 0, 0], 5000).into())?
        };
        let network_incoming = NetworkIncomingTask::new(
            endpoint,
            consensus.tx_request.clone(),
            network_outgoing.tx_connection.clone(),
        );
        Ok(Self {
            network_outgoing,
            execute,
            storage,
            consensus,
            network_incoming,
            _temp_dir: temp_dir,
        })
    }

    pub async fn run(self, stop: CancellationToken) -> anyhow::Result<()> {
        tokio::try_join!(
            self.network_outgoing.run(stop.clone()),
            self.execute.run(stop.clone()),
            self.consensus.run(stop.clone()),
            self.storage.run(stop.clone()),
            self.network_incoming.run(stop.clone()),
        )?;
        Ok(())
    }
}
