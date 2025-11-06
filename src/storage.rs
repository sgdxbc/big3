use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};

pub struct StorageWorkersChannels {
    pub tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    pub rx_fetch: flume::Receiver<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,

    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
    pub rx_post: UnboundedReceiver<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

pub struct StorageWorkersHandle {
    pub tx_fetch: flume::Sender<(Vec<u8>, oneshot::Sender<Option<Vec<u8>>>)>,
    pub tx_post: UnboundedSender<Vec<(Vec<u8>, Option<Vec<u8>>)>>,
}

impl Default for StorageWorkersChannels {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageWorkersChannels {
    pub fn new() -> Self {
        let (tx_fetch, rx_fetch) = flume::unbounded();
        let (tx_post, rx_post) = unbounded_channel();
        Self {
            tx_fetch,
            rx_fetch,
            tx_post,
            rx_post,
        }
    }

    pub fn handle(&self) -> StorageWorkersHandle {
        StorageWorkersHandle {
            tx_fetch: self.tx_fetch.clone(),
            tx_post: self.tx_post.clone(),
        }
    }
}
