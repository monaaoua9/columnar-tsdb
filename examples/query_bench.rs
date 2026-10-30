// cargo run --release --example query_bench
// 100 series x 100k points (10s apart, ~11 days), then a few queries, a delete and a regex match
use std::time::Instant;

use columnar_tsdb::query::{self, Ctx};
use columnar_tsdb::{Config, Engine, Point, Selector, SeriesKey};

const SERIES: usize = 100;
const PTS: i64 = 100_000;
const STEP_MS: i64 = 10_000;

fn time<T>(name: &str, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let r = f();
    println!("{name:<44} {:>9.2} ms", t.elapsed().as_secs_f64() * 1e3);
    r
}

fn main() {
    let dir = std::env::temp_dir().join("ctsdb-qbench");
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = Config::new(&dir);
    cfg.bucket_width = 24 * 3600 * 1000;
    let e = Engine::open(cfg).unwrap();

    time("ingest 10M points", || {
        for s in 0..SERIES {
            let k = SeriesKey::new("cpu")
                .label("host", format!("h{s:03}"))
                .label("dc", ["fra", "ams", "iad", "sin"][s % 4]);
            let pts: Vec<Point> = (0..PTS)
                .map(|i| Point::new(i * STEP_MS, ((i + s as i64) % 977) as f64 * 0.5))
                .collect();
            for c in pts.chunks(5_000) {
                e.append_batch(&k, c).unwrap();
            }
        }
        e.flush().unwrap();
    });

    let end = PTS * STEP_MS;
    let day = 24 * 3600 * 1000;
    let range = Ctx {
        start: end - 2 * day,
        end,
        step: Some(600_000),
        per_sec: 1000,
    };
    let inst = Ctx::instant(end - 1, end);

    for (name, q, ctx) in [
        ("instant: one series", r#"cpu{host="h007"}"#, &inst),
        ("sum by dc, 2 days @10m", "sum by (dc) (cpu)", &range),
        (
            "rate over 5m, all series, 2 days @10m",
            "rate(cpu[5m])",
            &range,
        ),
        (
            "topk(5) avg_over_time 1h",
            "topk(5, avg_over_time(cpu[1h]))",
            &range,
        ),
        (
            "regex host=~h0[0-4].",
            r#"count(cpu{host=~"h0[0-4]."})"#,
            &range,
        ),
    ] {
        time(name, || query::run(&e, q, ctx).unwrap());
    }

    let sel = Selector::new("cpu").eq("dc", "fra");
    let n = time("delete dc=fra, 1 day in the middle", || {
        e.delete_select(&sel, 5 * day, 6 * day).unwrap()
    });
    println!("  -> {n} points removed");

    drop(e);
    let _ = std::fs::remove_dir_all(&dir);
}
