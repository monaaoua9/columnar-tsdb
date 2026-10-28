// cargo run --release --example concurrent_bench
// N writer threads, one series each, small batches (the worst case for the shared wal lock).
use std::time::Instant;

use columnar_tsdb::{Config, Engine, Point, SeriesKey};

fn main() {
    let per_thread = 400_000i64;
    for threads in [1usize, 2, 4, 8] {
        let dir = std::env::temp_dir().join(format!("ctsdb-conc-{threads}"));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = Config::new(&dir);
        cfg.bucket_width = 24 * 3600 * 1000;
        if let Some(n) = std::env::var("WAL_SHARDS")
            .ok()
            .and_then(|s| s.parse().ok())
        {
            cfg.wal_shards = n;
        }
        let engine = Engine::open(cfg).unwrap();

        let t0 = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let e = engine.clone();
                std::thread::spawn(move || {
                    let k = SeriesKey::new("m").label("t", t.to_string());
                    let mut batch = Vec::with_capacity(256);
                    for i in 0..per_thread {
                        batch.push(Point::new(i * 10_000, (i % 977) as f64 * 0.5));
                        if batch.len() == 256 {
                            e.append_batch(&k, &batch).unwrap();
                            batch.clear();
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let secs = t0.elapsed().as_secs_f64();
        let total = threads as f64 * per_thread as f64;
        println!(
            "{threads} writer thread(s): {:.2} M pts/s total ({:.2} per thread)",
            total / secs / 1e6,
            total / secs / 1e6 / threads as f64
        );
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
