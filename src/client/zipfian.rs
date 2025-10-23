use rand::Rng;

/// Defaults from YCSB (precomputed zeta for fast path)
const ZIPFIAN_CONSTANT: f64 = 0.99;
const ZETAN: f64 = 26.469_028_201_783_02; // ζ(2^27, 0.99)

/// Zipfian random number generator (YCSB-compatible, using precomputed constants).
/// Draws from the inclusive range [base, base + items - 1].
pub struct Zipfian {
    base: u64,
    items: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

impl Zipfian {
    /// Create a generator over [min, max] with YCSB's default theta=0.99 (fast precomputed mode).
    pub fn new_range(min: u64, max: u64) -> Self {
        Self::new(min, max, ZIPFIAN_CONSTANT, ZETAN)
    }

    /// Exact mode (recomputes ζ(items, θ)); slower but precise.
    pub fn new_range_exact(min: u64, max: u64, theta: f64) -> Self {
        let zetan = zeta(max - min + 1, theta);
        Self::new(min, max, theta, zetan)
    }

    fn new(min: u64, max: u64, theta: f64, zetan: f64) -> Self {
        assert!(max >= min, "invalid range");
        let items = max - min + 1;
        let alpha = 1.0 / (1.0 - theta);
        let zeta2theta = zeta(2, theta);
        let eta = (1.0 - (2.0f64 / items as f64).powf(1.0 - theta)) / (1.0 - (zeta2theta / zetan));
        Self {
            base: min,
            items,
            theta,
            alpha,
            zetan,
            eta,
        }
    }

    /// Draw the next value using any external RNG.
    pub fn next_u64<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        // Identical branching to YCSB / Gray et al.
        let u: f64 = rng.random();
        let uz = u * self.zetan;

        if uz < 1.0 {
            return self.base;
        }
        if uz < 1.0 + (0.5f64).powf(self.theta) {
            return self.base + 1;
        }
        self.base
            + ((self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)).floor()
                as u64
    }

    /// Accessors
    pub fn items(&self) -> u64 {
        self.items
    }
    pub fn base(&self) -> u64 {
        self.base
    }
}

/// Scrambled Zipfian: hashes the Zipfian draw to distribute hot keys across the keyspace.
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

    pub fn new_range_exact(min: u64, max: u64, theta: f64) -> Self {
        let items = max - min + 1;
        let inner = Zipfian::new_range_exact(0, items - 1, theta);
        Self {
            inner,
            items,
            base: min,
        }
    }

    /// Draw using any external RNG; scrambling uses 64-bit FNV-1a on the raw Zipfian output.
    pub fn next_u64<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let v = self.inner.next_u64(rng);
        self.base + (fnv1a64(v) % self.items)
    }
}

/// ζ(n, θ) = Σ_{i=1..n} i^{-θ}
fn zeta(n: u64, theta: f64) -> f64 {
    let mut sum = 0.0;
    for i in 1..=n {
        sum += (i as f64).powf(-theta);
    }
    sum
}

/// 64-bit FNV-1a over the 8 bytes of the input (little-endian).
fn fnv1a64(mut x: u64) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x1000_0000_01B3;
    for _ in 0..8 {
        hash ^= x & 0xFF;
        hash = hash.wrapping_mul(PRIME);
        x >>= 8;
    }
    hash
}

/* --- Example usage ---
use rand::rngs::StdRng;
use rand::SeedableRng;

fn main() {
    let mut rng = StdRng::seed_from_u64(42);
    let z = Zipfian::new_range(0, 1_000_000 - 1);
    let s = ScrambledZipfian::new_range(0, 1_000_000 - 1);

    for _ in 0..5 {
        println!("{}", z.next_u64(&mut rng));
    }
    for _ in 0..5 {
        println!("{}", s.next_u64(&mut rng));
    }
}
*/
