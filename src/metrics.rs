use std::{fmt::Display, ops::AddAssign, time::Duration};

use hdrhistogram::Histogram;

pub struct Latency(Histogram<u64>, pub Duration);

impl Latency {
    pub fn new() -> Self {
        Self(Histogram::new(3).unwrap(), Duration::ZERO)
    }
}

impl Default for Latency {
    fn default() -> Self {
        Self::new()
    }
}

impl AddAssign<Duration> for Latency {
    fn add_assign(&mut self, other: Duration) {
        self.0 += other.as_nanos() as u64;
        self.1 += other;
    }
}

impl Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "len: {}, mean: {:?}, total: {:?}, p50: {:?}, p99: {:?}, max: {:?}",
            self.0.len(),
            Duration::from_nanos(self.0.mean() as _),
            self.1,
            Duration::from_nanos(self.0.value_at_quantile(0.5)),
            Duration::from_nanos(self.0.value_at_quantile(0.99)),
            Duration::from_nanos(self.0.max()),
        )
    }
}
