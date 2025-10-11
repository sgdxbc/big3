use crate::types::{ClientId, NodeIndex, Reply, Request};

pub trait ExecuteContext {
    fn send(&mut self, id: ClientId, reply: Reply);
}

pub struct Execute<C> {
    context: C,
    index: NodeIndex,
}

impl<C> Execute<C> {
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self { context, index }
    }
}

impl<C: ExecuteContext> Execute<C> {
    pub fn execute(&mut self, request: Request) {
        let reply = Reply {
            client_seq: request.client_seq,
            res: request.command, // echo
            node_index: self.index,
        };
        self.context.send(request.client_id, reply);
    }
}
