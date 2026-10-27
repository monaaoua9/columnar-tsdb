// cargo run --release --example ingest_bench
use std::time::Instant;

use columnar_tsdb::{Config, Engine, Point, SeriesKey};

fn main() {
    let dir = std::env::temp_dir().join("ctsdb-bench");
    let _ = std::fs::remove_dir_all(&dir);

    let mut cfg = Config::new(&dir);
    cfg.bucket_width = 2 * 3600 * 1000;
    let engine = Engine::open(cfg).unwrap();

    let n_series = 8;
    let per_series = 500_000i64;
    let keys: Vec<_> = (0..n_series)
        .map(|i| SeriesKey::new("cpu_usage").label("host", format!("h{i}")))
        .collect();

    // 10s scrape interval, slowly drifting gauge
    let t0 = Instant::now();
    let mut x = 40.0f64;
    let mut seed = 12345u64;
    for k in &keys {
        let mut batch = Vec::with_capacity(4096);
        for i in 0..per_series {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            x += ((seed % 200) as f64 - 100.0) / 500.0;
            batch.push(Point::new(
                1_700_000_000_000 + i * 10_000,
                (x * 100.0).round() / 100.0,
            ));
            if batch.len() == 4096 {
                engine.append_batch(k, &batch).unwrap();
                batch.clear();
            }
        }
        engine.append_batch(k, &batch).unwrap();
    }
    let total = n_series as i64 * per_series;
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "ingest: {total} points in {secs:.2}s = {:.2} M pts/s",
        total as f64 / secs / 1e6
    );

    engine.flush().unwrap();
    engine.compact().unwrap();
    let st = engine.stats();
    println!(
        "stored: {} chunks, {} points, {:.2} bytes/point ({:.1}x vs 16 B raw)",
        st.sealed_chunks,
        st.sealed_points,
        st.compressed_bytes as f64 / st.sealed_points as f64,
        16.0 * st.sealed_points as f64 / st.compressed_bytes as f64
    );

    let (lo, hi) = (1_700_000_000_000, 1_700_000_000_000 + per_series * 10_000);
    let t = Instant::now();
    let a = engine.aggregate(&keys[0], lo, hi).unwrap().unwrap();
    println!(
        "full-range aggregate (stats only): {:?}  avg={:.3}",
        t.elapsed(),
        a.avg().unwrap()
    );

    let t = Instant::now();
    let a = engine
        .aggregate(&keys[0], lo + 1_000_000_000, lo + 1_000_000_000 + 3_600_000)
        .unwrap();
    println!(
        "1h window aggregate (partial chunk decode): {:?}  n={}",
        t.elapsed(),
        a.unwrap().count
    );

    let t = Instant::now();
    let pts = engine.query_range(&keys[0], lo, hi).unwrap();
    println!("full scan: {} points in {:?}", pts.len(), t.elapsed());

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
