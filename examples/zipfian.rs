use big::workload::zipfian::ScrambledZipfian;
use hashbrown::HashMap;
use rand::rng;

fn main() {
    let num_keys = 100_000_000;
    let zipf_s = 0.99;
    let zipfian = ScrambledZipfian::new_range_exact(0, num_keys - 1, zipf_s);

    let num_samples = 100_000_000;
    let mut counts = HashMap::new();
    for _ in 0..num_samples {
        let key = zipfian.next_u64(&mut rng());
        *counts.entry(key).or_insert(0usize) += 1;
    }
    let mut counts_vec: Vec<(u64, usize)> = counts.into_iter().collect();
    counts_vec.sort_by_key(|&(_k, v)| std::cmp::Reverse(v));
    let mut acc = 0;
    let mut acc_percent = 0;
    for (i, (_k, v)) in counts_vec.iter().enumerate() {
        acc += *v;
        let new_acc_percent = (acc as f64) / (num_samples as f64) * 100.0;
        if new_acc_percent - acc_percent as f64 >= 1.0 {
            acc_percent = new_acc_percent as u32;
            println!(
                "acc_percent {:>.2}% Top {:>6} {}%",
                acc_percent,
                i + 1,
                ((i + 1) * 100) as f64 / num_keys as f64
            );
        }
    }
}
