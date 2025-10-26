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

use self::zipfian::ScrambledZipfian;

#[allow(unused)]
mod zipfian;

pub trait ClientContext {
    fn send(&mut self, to: NodeIndex, request: Request);
}

pub struct Client<C> {
    pub context: C,
    config: ClientConfig,
    id: ClientId,

    seq: ClientSeq,
    inflights: BTreeMap<ClientSeq, Inflight>,

    metrics: ClientMetrics,
}

struct Inflight {
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
            inflights: Default::default(),
            metrics: Default::default(),
        }
    }
}

impl<C: ClientContext> Client<C> {
    pub fn invoke(&mut self, command: Vec<u8>, tx_response: oneshot::Sender<Vec<u8>>) {
        self.seq += 1;

        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            command,
        };
        self.context.send(
            rng().random_range(0..(self.config.num_nodes - self.config.num_faulty_nodes)),
            request,
        );

        self.inflights.insert(
            self.seq,
            Inflight {
                replies: Default::default(),
                tx_response,
            },
        );
    }

    pub fn on_message(&mut self, message: Reply) {
        let Some(inflight) = self.inflights.get_mut(&message.client_seq) else {
            return;
        };
        inflight
            .replies
            .insert(message.node_index, message.res.clone());
        if inflight.replies.len() > self.config.num_faulty_nodes as usize
            && inflight
                .replies
                .values()
                .filter(|&res| res == &message.res)
                .count()
                == (self.config.num_faulty_nodes + 1) as usize
        {
            let ongoing = self.inflights.remove(&message.client_seq).unwrap();
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

    zipfian: ScrambledZipfian,
    inflights: HashMap<InvokeId, Instant>,
    pub records: Arc<Mutex<Records>>,
}

pub struct Records {
    pub start: Instant,
    pub latency_histogram: Histogram<u64>,
}

impl<C> ClientWorker<C> {
    pub fn new(context: C, config: ClientWorkerConfig) -> Self {
        let zipfian = ScrambledZipfian::new_range(0, config.num_keys - 1);
        Self {
            context,
            config,
            zipfian,
            inflights: Default::default(),
            records: Arc::new(Mutex::new(Records {
                start: Instant::now(),
                latency_histogram: Histogram::new(3).unwrap(),
            })),
        }
    }
}

impl<C: ClientWorkerContext> ClientWorker<C> {
    pub fn start(&mut self) {
        for _ in 0..self.config.num_concurrent {
            self.invoke();
        }
    }

    pub fn on_invoke_response(&mut self, invoke_id: InvokeId, _res: Vec<u8>) {
        let Some(start) = self.inflights.remove(&invoke_id) else {
            unimplemented!()
        };
        self.records.lock().unwrap().latency_histogram += start.elapsed().as_nanos() as u64;
        self.invoke();
    }

    fn invoke(&mut self) {
        let key = execute::key(self.zipfian.next_u64(&mut rng()));
        // let key = execute::key(0);
        // let key = execute::key(rng().random_range(0..self.config.num_keys));
        let op = if rng().random_bool(self.config.read_ratio) {
            Op::Get(key)
        } else {
            let mut value = vec![0; 100 - 16];
            rng().fill_bytes(&mut value);
            Op::Put(key, value)
        };
        let command = bincode::encode_to_vec(&op, bincode::config::standard()).unwrap();
        let invoke_id = self.context.invoke(command);
        self.inflights.insert(invoke_id, Instant::now());
    }
}
