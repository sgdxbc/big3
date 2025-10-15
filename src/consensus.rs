use crate::types::{NodeIndex, Request};

pub trait ConsensusContext {
    fn execute(&mut self, requests: Vec<Request>);
}

pub struct Consensus<C> {
    context: C,
    #[allow(unused)]
    index: NodeIndex,
}

impl<C> Consensus<C> {
    pub fn new(context: C, index: NodeIndex) -> Self {
        Self { context, index }
    }
}

impl<C: ConsensusContext> Consensus<C> {
    pub fn on_request(&mut self, request: Request) {
        self.context.execute(vec![request]);
    }
}
