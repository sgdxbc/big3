use std::collections::HashMap;

use log::warn;

use crate::{
    schema::ClientConfig,
    tasks::ResponseContext,
    types::{ClientId, ClientSeq, NodeIndex, Reply, Request},
};

pub trait ClientContext {
    fn send(&mut self, request: Request); // to a random replica
}

pub struct Client<C> {
    pub context: C,
    config: ClientConfig,
    id: ClientId,

    seq: ClientSeq,
    replies: HashMap<NodeIndex, Vec<u8>>,
    tx_response: Option<ResponseContext<Vec<u8>>>,
}

impl<C> Client<C> {
    pub fn new(context: C, config: ClientConfig, id: ClientId) -> Self {
        Self {
            context,
            config,
            id,
            seq: 0,
            replies: Default::default(),
            tx_response: None,
        }
    }
}

impl<C: ClientContext> Client<C> {
    pub fn invoke(&mut self, command: Vec<u8>, tx_response: ResponseContext<Vec<u8>>) {
        assert!(self.tx_response.is_none());
        self.seq += 1;

        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            command,
        };
        self.context.send(request);

        self.tx_response = Some(tx_response);
        self.replies.clear();
    }

    pub fn on_message(&mut self, message: Reply) {
        assert!(message.client_seq <= self.seq);
        if message.client_seq < self.seq || self.tx_response.is_none() {
            // warn!(
            //     "stale reply for client_seq {} (current {})",
            //     message.client_seq, self.seq
            // );
            return;
        }

        self.replies.insert(message.node_index, message.res.clone());
        if self.replies.len() > self.config.num_faulty_nodes as usize {
            if self
                .replies
                .values()
                .filter(|&res| res == &message.res)
                .count()
                >= (self.config.num_faulty_nodes + 1) as usize
            {
                self.tx_response.take().unwrap().respond(message.res);
            } else {
                warn!(
                    "received non-matching replies for client_seq {}",
                    message.client_seq
                );
            }
        }
    }

    pub fn log_metrics(&self) {}
}
