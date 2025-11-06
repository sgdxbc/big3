use bincode::{Decode, Encode};
use tokio::sync::mpsc::UnboundedSender;

pub type NodeIndex = big_schema::NodeIndex;
pub type ClientId = u32;
pub type ClientSeq = u64;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request {
    pub client_id: ClientId,
    pub client_seq: ClientSeq,
    pub command: Vec<u8>,
}

#[derive(Debug, Encode, Decode)]
pub struct Reply {
    pub client_seq: ClientSeq,
    pub res: Vec<u8>,
    pub node_index: NodeIndex,
}

pub type RequestId = u64;

pub struct RequestContext<R, P> {
    id: RequestId,
    tx_request: UnboundedSender<(R, ResponseContext<P>)>,
    tx_response: UnboundedSender<(RequestId, P)>,
}

impl<R, P> RequestContext<R, P> {
    pub fn new(
        tx_request: UnboundedSender<(R, ResponseContext<P>)>,
        tx_response: UnboundedSender<(RequestId, P)>,
    ) -> Self {
        Self {
            id: 0,
            tx_request,
            tx_response,
        }
    }

    pub fn request(&mut self, request: R) -> RequestId {
        self.id += 1;
        let ctx = ResponseContext {
            id: self.id,
            tx: self.tx_response.clone(),
        };
        let _ = self.tx_request.send((request, ctx));
        self.id
    }
}

pub struct ResponseContext<T> {
    id: RequestId,
    tx: UnboundedSender<(RequestId, T)>,
}

impl<T> ResponseContext<T> {
    pub fn new(id: RequestId, tx: UnboundedSender<(RequestId, T)>) -> Self {
        Self { id, tx }
    }

    pub fn respond(self, response: T) {
        let _ = self.tx.send((self.id, response));
    }
}

pub const PREFILL_PATH: &str = "/tmp/big-prefill";
