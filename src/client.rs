use std::collections::{BTreeMap, HashMap};

use crate::{
    schema::ClientConfig,
    types::{ClientId, ClientSeq, NodeIndex, Reply, Request},
};

pub trait ClientContext {
    fn send(&mut self, to: NodeIndex, request: Request);
    fn finalize(&mut self, seq: ClientSeq, res: Vec<u8>);
}

pub struct Client<C> {
    context: C,
    config: ClientConfig,
    id: ClientId,

    seq: ClientSeq,
    ongoing: BTreeMap<ClientSeq, Ongoing>,
}

struct Ongoing {
    replies: HashMap<NodeIndex, Vec<u8>>,
    // save command if resending
}

impl<C> Client<C> {
    pub fn new(context: C, config: ClientConfig, id: ClientId) -> Self {
        Self {
            context,
            config,
            id,
            seq: 0,
            ongoing: Default::default(),
        }
    }
}

impl<C: ClientContext> Client<C> {
    pub fn invoke(&mut self, command: Vec<u8>) -> ClientSeq {
        self.seq += 1;

        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            command,
        };
        // TODO randomize
        self.context.send(0, request);

        self.ongoing.insert(
            self.seq,
            Ongoing {
                replies: Default::default(),
            },
        );
        while self.ongoing.len() > self.config.num_max_ongoing {
            self.ongoing.pop_first();
        }

        self.seq
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
            self.ongoing.remove(&message.client_seq);
            self.context.finalize(message.client_seq, message.res);
        }
    }
}

pub struct ClientWorker<C> {
    client: Client<C>,
    // config
}

impl<C> ClientWorker<C> {
    pub fn new(client: Client<C>) -> Self {
        Self { client }
    }
}

impl<C: ClientContext> ClientWorker<C> {
    pub fn init(&mut self) {
        self.client.invoke(vec![]); // TODO
    }

    pub fn on_finalize(&mut self, _seq: ClientSeq, _res: Vec<u8>) {
        self.client.invoke(vec![]); // TODO
    }
}
