#[allow(unused)]
mod defaults;

#[allow(unused)]
pub use defaults::*;

include!("configs/overrides.rs");

pub const fn num_nodes() -> u16 {
    NUM_FAULTY_NODES * 3 + 1
}
