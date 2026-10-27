use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use columnar_tsdb::compress::{gorilla, timestamp};
use columnar_tsdb::{Config, Engine, Point, SeriesKey};

const N: usize = 100_000;

fn data() -> (Vec<i64>, Vec<f64>) {
    let mut seed = 42u64;
    let mut x = 40.0f64;
    let mut t = 1_700_000_000_000i64;
    let (mut ts, mut vs) = (Vec::with_capacity(N), Vec::with_capacity(N));
    for _ in 0..N {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        t += 10_000 + (seed % 5) as i64 - 2;
        x += ((seed >> 8) % 200) as f64 / 1000.0 - 0.1;
        ts.push(t);
        vs.push((x * 100.0).round() / 100.0);
    }
    (ts, vs)
}

fn codecs(c: &mut Criterion) {
    let (ts, vs) = data();
    let ts_enc = timestamp::encode(&ts);
    let vs_enc = gorilla::encode(&vs);

    let mut g = c.benchmark_group("codec");
    g.throughput(Throughput::Elements(N as u64));
    g.bench_function("timestamp_encode", |b| {
        b.iter(|| timestamp::encode(black_box(&ts)))
    });
    g.bench_function("timestamp_decode", |b| {
        b.iter(|| timestamp::decode(black_box(&ts_enc), usize::MAX).unwrap())
    });
    g.bench_function("gorilla_encode", |b| {
        b.iter(|| gorilla::encode(black_box(&vs)))
    });
    g.bench_function("gorilla_decode", |b| {
        b.iter(|| gorilla::decode(black_box(&vs_enc), usize::MAX).unwrap())
    });
    g.finish();
}

fn ingest(c: &mut Criterion) {
    let (ts, vs) = data();
    let pts: Vec<Point> = ts
        .iter()
        .zip(&vs)
        .map(|(&t, &v)| Point::new(t, v))
        .collect();
    let key = SeriesKey::new("bench").label("host", "a");
    let base = std::env::temp_dir().join(format!("ctsdb-crit-{}", std::process::id()));

    let mut g = c.benchmark_group("engine");
    g.throughput(Throughput::Elements(N as u64));
    g.sample_size(10);
    g.warm_up_time(std::time::Duration::from_millis(500));
    g.measurement_time(std::time::Duration::from_secs(4));

    // fresh engine + dir per iteration so disk use stays bounded; open cost is outside the timing
    let mut n = 0u64;
    g.bench_function("append_batch_100k", |b| {
        b.iter_batched(
            || {
                n += 1;
                let dir = base.join(n.to_string());
                (Engine::open(Config::new(&dir)).unwrap(), dir)
            },
            |(engine, dir)| {
                engine.append_batch(&key, black_box(&pts)).unwrap();
                (engine, dir)
            },
            criterion::BatchSize::PerIteration,
        )
    });
    g.finish();
    let _ = std::fs::remove_dir_all(&base);
}

criterion_group!(benches, codecs, ingest);
criterion_main!(benches);
