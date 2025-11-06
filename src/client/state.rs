use std::collections::{HashMap, hash_map::Entry};

use log::warn;

use crate::{
    common::{ClientId, ClientSeq, NodeIndex, Reply, Request, ResponseContext},
    schema::ClientConfig,
};

pub trait ClientContext {
    fn send(&mut self, request: Request); // to a random replica
}

pub struct Client<C> {
    pub context: C,
    config: ClientConfig,
    id: ClientId,

    seq: ClientSeq,
    working_states: HashMap<ClientSeq, WorkingState>,
}

struct WorkingState {
    replies: HashMap<NodeIndex, Vec<u8>>,
    context: ResponseContext<Vec<u8>>,
}

impl<C> Client<C> {
    pub fn new(context: C, config: ClientConfig, id: ClientId) -> Self {
        Self {
            context,
            config,
            id,
            seq: 0,
            working_states: Default::default(),
        }
    }
}

impl<C: ClientContext> Client<C> {
    pub fn invoke(&mut self, command: Vec<u8>, context: ResponseContext<Vec<u8>>) {
        self.seq += 1;

        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            command,
        };
        self.context.send(request);

        self.working_states.insert(
            self.seq,
            WorkingState {
                replies: Default::default(),
                context,
            },
        );
    }

    pub fn on_message(&mut self, message: Reply) {
        assert!(message.client_seq <= self.seq);
        let Entry::Occupied(mut state) = self.working_states.entry(message.client_seq) else {
            warn!(
                "stale reply for client_seq {} (no working state)",
                message.client_seq
            );
            return;
        };

        state
            .get_mut()
            .replies
            .insert(message.node_index, message.res.clone());
        if state.get().replies.len() > self.config.num_faulty_nodes as usize {
            if state
                .get()
                .replies
                .values()
                .filter(|&res| res == &message.res)
                .count()
                >= (self.config.num_faulty_nodes + 1) as usize
            {
                state.remove().context.respond(message.res);
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
