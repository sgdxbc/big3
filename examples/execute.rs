use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::Arc,
    time::{Duration, Instant},
};

use log::info;
use rand::{Rng, rng};
use rocksdb::{DB, WriteBatch};
use rustc_hash::FxHashMap;
use tempfile::TempDir;
use tokio::{
    process::Command,
    runtime, select,
    sync::oneshot,
    task::{JoinSet, spawn_blocking},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

const NUM_MAX_CONCURRENT_OP: usize = 100;
const READ_RATIO: f64 = 0.95;
const NUM_KEYS: u64 = 1_000_000_000;
const NUM_GET_WORKER_THREADS: usize = 20;
const DURATION: Duration = Duration::from_secs(20);

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let temp_dir = TempDir::new()?;

    let status = Command::new("cp")
        .args(["-rT", "/tmp/big-prefill"])
        .arg(temp_dir.path())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "cp command failed");

    let db = Arc::new(DB::open_default(temp_dir.path())?);

    let (tx_op, rx_op) = flume::unbounded();
    let (tx_res, rx_res) = flume::unbounded();
    let (tx_get, rx_get) = flume::unbounded();
    let (tx_updates, rx_updates) = flume::bounded(1000);
    let (tx_write_done, rx_write_done) = flume::unbounded();
    let token = CancellationToken::new();

    let source = Source::new(rx_res, tx_op, tx_get);
    let worker = Worker::new(db, rx_get, rx_updates, tx_write_done);
    let sched = Sched::new(Execute::new(tx_res), rx_op, rx_write_done, tx_updates);
    let timeout = async {
        sleep(DURATION).await;
        token.cancel();
        anyhow::Ok(())
    };
    tokio::try_join!(
        source.run(token.clone()),
        worker.run(),
        sched.run(token.clone()),
        timeout,
    )?;

    temp_dir.close()?;
    Ok(())
}

fn get_worker(
    db: Arc<DB>,
    rx_key: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
) -> anyhow::Result<()> {
    while let Ok((key, tx)) = rx_key.recv() {
        let value = db.get(key)?;
        let _ = tx.send(value);
    }
    Ok(())
}

fn write_worker(
    db: Arc<DB>,
    rx_updates: flume::Receiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    tx_write_done: flume::Sender<u64>,
) -> anyhow::Result<()> {
    let mut count = 0;
    while let Ok(updates) = rx_updates.recv() {
        let mut batch = WriteBatch::new();
        for (key, value) in updates {
            match value {
                Some(v) => batch.put(key, v),
                None => batch.delete(key),
            }
        }
        count += 1;
        for updates in rx_updates.try_iter() {
            for (key, value) in updates {
                match value {
                    Some(v) => batch.put(key, v),
                    None => batch.delete(key),
                }
            }
            count += 1;
        }

        db.write(batch)?;
        let _ = tx_write_done.send(count);
    }
    Ok(())
}

struct Worker {
    db: Arc<DB>,

    rx_get: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    rx_updates: flume::Receiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    tx_write_done: flume::Sender<u64>,
}

fn set_affinity(#[allow(unused_variables)] i: usize) {
    #[cfg(target_os = "linux")]
    {
        let mut cpu_set = rustix::thread::CpuSet::new();
        cpu_set.set(i);
        rustix::thread::sched_setaffinity(None, &cpu_set).unwrap();
    }
}

impl Worker {
    fn new(
        db: Arc<DB>,
        rx_get: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
        rx_updates: flume::Receiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
        tx_write_done: flume::Sender<u64>,
    ) -> Self {
        Self {
            db,
            rx_get,
            rx_updates,
            tx_write_done,
        }
    }

    async fn run(self) -> anyhow::Result<()> {
        let mut join_set = JoinSet::new();
        for i in 0..NUM_GET_WORKER_THREADS {
            let db = self.db.clone();
            let rx_get = self.rx_get.clone();
            join_set.spawn_blocking(move || {
                set_affinity(i % 5);
                get_worker(db, rx_get)
            });
        }
        join_set.spawn_blocking(move || {
            set_affinity(5);
            write_worker(self.db, self.rx_updates, self.tx_write_done)
        });
        while let Some(res) = join_set.join_next().await {
            res??;
        }
        Ok(())
    }
}

const VALUE_SIZE: usize = 100 - 16;

fn key(index: u64) -> Vec<u8> {
    format!("key-{index:012}").into_bytes()
}

trait AbstractOp {
    fn read_set(&self) -> Vec<Vec<u8>>;
}

trait AbstractExecute {
    type Op: AbstractOp;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)>;
}

enum Op {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
}

enum Res {
    Put,
    #[allow(dead_code)]
    Get(Option<Vec<u8>>),
}

impl AbstractOp for Op {
    fn read_set(&self) -> Vec<Vec<u8>> {
        match self {
            Self::Put(_, _) => vec![],
            Self::Get(key) => vec![key.clone()],
        }
    }
}

struct Execute {
    tx_res: flume::Sender<Res>,
}

impl Execute {
    fn new(tx_res: flume::Sender<Res>) -> Self {
        Self { tx_res }
    }
}

impl AbstractExecute for Execute {
    type Op = Op;

    fn execute(
        &mut self,
        op: Self::Op,
        state: FxHashMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        match op {
            Op::Put(key, value) => {
                let _ = self.tx_res.send(Res::Put);
                vec![(key, Some(value))]
            }
            Op::Get(key) => {
                let _ = self.tx_res.send(Res::Get(state[&key].clone()));
                vec![]
            }
        }
    }
}

struct Source {
    rx_res: flume::Receiver<Res>,

    tx_op: flume::Sender<OpState<Op>>,
    tx_get: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
}

impl Source {
    fn new(
        rx_res: flume::Receiver<Res>,
        tx_op: flume::Sender<OpState<Op>>,
        tx_get: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    ) -> Self {
        Self {
            rx_res,
            tx_op,
            tx_get,
        }
    }

    async fn run(self, token: CancellationToken) -> anyhow::Result<()> {
        spawn_blocking(move || {
            set_affinity(7);
            runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async move { token.run_until_cancelled(self.run_loop()).await })
        })
        .await?;
        Ok(())
    }

    async fn run_loop(&self) {
        info!("Source started");
        for _ in 0..NUM_MAX_CONCURRENT_OP {
            self.request();
        }
        let mut count = 0;
        let start = Instant::now();
        while let Ok(_res) = self.rx_res.recv_async().await {
            count += 1;
            if count % 100_000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let qps = count as f64 / elapsed;
                info!("Completed {count} ops in {elapsed:.2} s ({qps:.2} ops/s)");
            }

            self.request();
        }
        info!("Source finished");
    }

    fn request(&self) {
        let key = key(rng().random_range(0..NUM_KEYS));
        let op = if rng().random_bool(READ_RATIO) {
            Op::Get(key)
        } else {
            let mut value = vec![0u8; VALUE_SIZE];
            rng().fill(&mut value[..]);
            Op::Put(key, value)
        };
        let mut read_set = FxHashMap::default();
        for key in op.read_set() {
            let (tx, rx) = oneshot::channel();
            let _ = self.tx_get.send((key.clone(), tx));
            read_set.insert(key, rx);
        }
        let op_state = OpState { op, read_set };
        let _ = self.tx_op.send(op_state);
    }
}

struct Sched<E: AbstractExecute> {
    executor: E,
    recent_updates: FxHashMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    current_version: u64,
    evict_queue: BinaryHeap<Reverse<(u64, Vec<u8>)>>,

    rx_op: flume::Receiver<OpState<E::Op>>,
    rx_write_done: flume::Receiver<u64>,
    tx_write: flume::Sender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

struct OpState<Op> {
    op: Op,
    read_set: FxHashMap<Vec<u8>, oneshot::Receiver<Option<Vec<u8>>>>,
}

impl<E: AbstractExecute> Sched<E> {
    fn new(
        executor: E,
        rx_op: flume::Receiver<OpState<E::Op>>,
        rx_write_done: flume::Receiver<u64>,
        tx_write: flume::Sender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    ) -> Self {
        Self {
            executor,
            recent_updates: FxHashMap::default(),
            current_version: 0,
            evict_queue: BinaryHeap::new(),

            rx_op,
            rx_write_done,
            tx_write,
        }
    }

    async fn run(mut self, token: CancellationToken) -> anyhow::Result<()>
    where
        E: Send + 'static,
        E::Op: Send + 'static,
    {
        spawn_blocking(move || {
            set_affinity(6);
            runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async move { token.run_until_cancelled(self.run_loop()).await })
        })
        .await?;
        Ok(())
    }

    async fn run_loop(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                Ok(op) = self.rx_op.recv_async() => self.handle_op(op).await?,
                Ok(n) = self.rx_write_done.recv_async() => self.handle_write_done(n),
            }
        }
    }

    async fn handle_op(&mut self, op_state: OpState<E::Op>) -> anyhow::Result<()> {
        let mut state = FxHashMap::default();
        for (key, rx_value) in op_state.read_set {
            let value = if let Some((_, value)) = self.recent_updates.get(&key) {
                value.clone()
            } else {
                rx_value.await?
            };
            state.insert(key, value);
        }
        let updates = self.executor.execute(op_state.op, state);

        if !updates.is_empty() {
            self.current_version += 1;
            for (key, value) in &updates {
                self.recent_updates
                    .insert(key.clone(), (self.current_version, value.clone()));
                self.evict_queue
                    .push(Reverse((self.current_version, key.clone())));
            }
            let _ = self.tx_write.send_async(updates).await;
        }
        Ok(())
    }

    fn handle_write_done(&mut self, version: u64) {
        while self
            .evict_queue
            .peek()
            .is_some_and(|&Reverse((v, _))| v <= version)
        {
            let Reverse((v, key)) = self.evict_queue.pop().unwrap();
            if self.recent_updates[&key].0 == v {
                self.recent_updates.remove(&key);
            }
        }
    }
}
