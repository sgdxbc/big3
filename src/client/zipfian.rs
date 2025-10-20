use rand::Rng;

/// Zipfian random number generator (faithful to YCSB’s ZipfianGenerator).
/// Draws from [base, base+items).
pub struct Zipfian {
    items: u64,
    base: u64,
    theta: f64,
    zeta2theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    count_for_zeta: u64,
}

impl Zipfian {
    pub const ZIPFIAN_CONSTANT: f64 = 0.99;

    /// Construct over an inclusive range [min, max].
    pub fn new_range(min: u64, max: u64) -> Self {
        Self::new_range_with_theta(min, max, Self::ZIPFIAN_CONSTANT)
    }

    /// Same as new_range, but let caller pick theta.
    pub fn new_range_with_theta(min: u64, max: u64, theta: f64) -> Self {
        assert!(max >= min, "invalid range");
        let items = max - min + 1;
        let base = min;

        let zeta2theta = zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let zetan = zeta(items, theta);
        let eta = (1.0 - (2.0f64 / items as f64).powf(1.0 - theta)) / (1.0 - (zeta2theta / zetan));

        Self {
            items,
            base,
            theta,
            zeta2theta,
            alpha,
            zetan,
            eta,
            count_for_zeta: items,
        }
    }

    /// Draw next value.
    pub fn next_u64(&mut self, rng: &mut impl Rng) -> u64 {
        // Identical branch structure to YCSB (Gray et al. method).
        let u: f64 = rng.random();
        let uz = u * self.zetan;

        if uz < 1.0 {
            return self.base;
        }
        if uz < 1.0 + (0.5f64).powf(self.theta) {
            return self.base + 1;
        }
        let val = self.base
            + ((self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)).floor()
                as u64;
        val
    }

    /// (Optional) Incremental re-computation if the domain grows.
    /// Matches the spirit of YCSB's "countforzeta" optimization.
    pub fn ensure_items_at_least(&mut self, new_items: u64) {
        if new_items <= self.count_for_zeta {
            return;
        }
        for i in (self.count_for_zeta + 1)..=new_items {
            self.zetan += 1.0 / (i as f64).powf(self.theta);
        }
        self.count_for_zeta = new_items;
        self.items = new_items;
        self.eta = (1.0 - (2.0f64 / new_items as f64).powf(1.0 - self.theta))
            / (1.0 - (self.zeta2theta / self.zetan));
    }
}

/// Scrambled Zipfian: hashes the Zipfian draw to distribute "hot" keys across the keyspace.
pub struct ScrambledZipfian {
    inner: Zipfian,
    items: u64,
    base: u64,
}

impl ScrambledZipfian {
    pub fn new_range(min: u64, max: u64) -> Self {
        let items = max - min + 1;
        let inner = Zipfian::new_range(0, items - 1);
        Self {
            inner,
            items,
            base: min,
        }
    }

    pub fn new_range_with_theta(min: u64, max: u64, theta: f64) -> Self {
        let items = max - min + 1;
        let inner = Zipfian::new_range_with_theta(0, items - 1, theta);
        Self {
            inner,
            items,
            base: min,
        }
    }

    pub fn next_u64(&mut self, rng: &mut impl Rng) -> u64 {
        let v = self.inner.next_u64(rng);
        let h = fnv1a64(v);
        self.base + (h % self.items)
    }
}

/// Plain zeta(n, theta) = sum_{i=1..n} 1 / i^theta
fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 1..=n {
        sum += 1.0 / (i as f64).powf(theta);
    }
    sum
}

/// 64-bit FNV-1a over the 8 bytes of the input (little-endian), like YCSB’s usage.
fn fnv1a64(mut x: u64) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    for _ in 0..8 {
        hash ^= x & 0xff;
        hash = hash.wrapping_mul(PRIME);
        x >>= 8;
    }
    hash
}
