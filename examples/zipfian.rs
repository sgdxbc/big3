use big::workload::zipfian::ScrambledZipfian;
use hashbrown::HashMap;
use rand::rng;

fn main() {
    let num_keys = 100_000_000;
    let zipf_s = 1.24;
    let zipfian = ScrambledZipfian::new_range_exact(0, num_keys - 1, zipf_s);

    let num_samples = 100_000_000;
    let mut counts = HashMap::new();
    for _ in 0..num_samples {
        let key = zipfian.next_u64(&mut rng());
        *counts.entry(key).or_insert(0usize) += 1;
    }
    let mut acc = 0;
    for i in 0..10_000_000 {
        let v = counts.get(&(zipfian.scramble(i))).unwrap_or(&0);
        acc += *v;
        if (i + 1) % 100_000 == 0 {
            println!(
                "key {:>8} ({:.4}%): cumulative {:>8} ({:.4}%)",
                i + 1,
                (i + 1) as f64 / num_keys as f64 * 100.0,
                acc,
                acc as f64 / num_samples as f64 * 100.0
            );
        }
    }
}
