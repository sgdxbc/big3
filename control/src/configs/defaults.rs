pub const NUM_KEYS: u64 = 10_000_000;
pub const READ_RATIO: f64 = 0.5;
pub const NUM_FAULTY_NODES: u16 = 1;

pub const fn num_nodes() -> u16 {
    NUM_FAULTY_NODES * 3 + 1
}
