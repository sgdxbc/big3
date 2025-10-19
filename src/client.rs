use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Instant,
};

use hdrhistogram::Histogram;
use log::info;
use rand::{Rng, RngCore as _, rng};
use tokio::sync::oneshot;

use crate::{
    execute::{self, Op},
    schema::{ClientConfig, ClientWorkerConfig},
    types::{ClientId, ClientSeq, NodeIndex, Reply, Request},
};

pub trait ClientContext {
    fn send(&mut self, to: NodeIndex, request: Request);
}

pub struct Client<C> {
    pub context: C,
    config: ClientConfig,
    id: ClientId,

    seq: ClientSeq,
    ongoing: BTreeMap<ClientSeq, Ongoing>,

    metrics: ClientMetrics,
}

struct Ongoing {
    replies: HashMap<NodeIndex, Vec<u8>>,
    tx_response: oneshot::Sender<Vec<u8>>,
    // save command if resending
}

#[derive(Default)]
struct ClientMetrics {
    commit_count: u64,
}

impl<C> Client<C> {
    pub fn new(context: C, config: ClientConfig, id: ClientId) -> Self {
        Self {
            context,
            config,
            id,
            seq: 0,
            ongoing: Default::default(),
            metrics: Default::default(),
        }
    }

    const NUM_MAX_ONGOING: usize = 100_000;
}

impl<C: ClientContext> Client<C> {
    pub fn invoke(&mut self, command: Vec<u8>, tx_response: oneshot::Sender<Vec<u8>>) {
        self.seq += 1;

        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            command,
        };
        self.context
            .send(rng().random_range(0..self.config.num_nodes), request);

        self.ongoing.insert(
            self.seq,
            Ongoing {
                replies: Default::default(),
                tx_response,
            },
        );
        while self.ongoing.len() > Self::NUM_MAX_ONGOING {
            self.ongoing.pop_first();
        }
    }

    pub fn on_message(&mut self, message: Reply) {
        let Some(ongoing) = self.ongoing.get_mut(&message.client_seq) else {
            return;
        };
        ongoing
            .replies
            .insert(message.node_index, message.res.clone());
        if ongoing
            .replies
            .values()
            .filter(|&res| res == &message.res)
            .count()
            == (self.config.num_faulty_nodes + 1) as usize
        {
            let ongoing = self.ongoing.remove(&message.client_seq).unwrap();
            let _ = ongoing.tx_response.send(message.res);
            self.metrics.commit_count += 1;
        }
    }

    pub fn log_metrics(&self) {
        let commit_rate = self.metrics.commit_count as f64 / self.seq as f64;
        info!("commit rate: {:.2}%", commit_rate * 100.0);
    }
}

pub type InvokeId = u64;

pub trait ClientWorkerContext {
    fn invoke(&mut self, command: Vec<u8>) -> InvokeId;
}

pub struct ClientWorker<C> {
    pub context: C,
    config: ClientWorkerConfig,

    start: Instant,
    invoke_count: u64,
    ongoing: HashMap<InvokeId, Instant>,
    pub records: Arc<Mutex<Records>>,
}

pub struct Records {
    pub start: Instant,
    pub latency_histogram: Histogram<u64>,
}

impl<C> ClientWorker<C> {
    pub fn new(context: C, config: ClientWorkerConfig) -> Self {
        Self {
            context,
            config,
            start: Instant::now(), // placeholder
            invoke_count: 0,
            ongoing: Default::default(),
            records: Arc::new(Mutex::new(Records {
                start: Instant::now(),
                latency_histogram: Histogram::new(3).unwrap(),
            })),
        }
    }
}

impl<C: ClientWorkerContext> ClientWorker<C> {
    pub fn start(&mut self) {
        self.start = Instant::now();
    }

    pub fn on_tick(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let target_invoke_count = (self.config.rate * elapsed).floor() as u64;
        while self.invoke_count < target_invoke_count {
            self.invoke();
            self.invoke_count += 1;
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, _res: Vec<u8>) {
        let Some(start) = self.ongoing.remove(&invoke_id) else {
            unimplemented!()
        };
        self.records.lock().unwrap().latency_histogram += start.elapsed().as_nanos() as u64;
    }

    fn invoke(&mut self) {
        let key = execute::key(rng().random_range(0..self.config.num_keys));
        let op = if rng().random_bool(self.config.read_ratio) {
            Op::Get(key)
        } else {
            let mut value = vec![0; 100 - 32];
            rng().fill_bytes(&mut value);
            Op::Put(key, value)
        };
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        let invoke_id = self.context.invoke(command);
        self.ongoing.insert(invoke_id, Instant::now());
    }
}
