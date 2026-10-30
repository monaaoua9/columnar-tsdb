# columnar-tsdb: benchmarks

Measured 2026-10-29.

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD |
| OS | Windows 11 |
| Toolchain | Rust 1.99.1, `--release` |

## Ingest and queries

```
cargo run --release --example ingest_bench
```

8 series × 500,000 points, 10 s scrape interval, 1-day buckets, a noisy 2-decimal gauge. The ingest figure is sustained: writers are throttled by the sealer, so it includes writing the chunk files. Three runs; median with min–max in brackets.

| | |
|---|---|
| ingest | 6.82 M pts/s (6.41 – 7.44) |
| stored | 472 chunks, 6.14 bytes/point (2.6× vs 16 B raw), identical in all runs |
| full-range aggregate, stats only | 5.1 µs (4.4 – 5.6) |
| 1 h window aggregate (partial chunk decode) | 18.4 ms (15.8 – 30.8) |
| full scan of one series, 500k points | 1.28 s (1.25 – 2.39) |

### The two slow rows are mostly the operating system

The 1 h window and the full scan decode chunk files that were written seconds earlier, and they're 10–14× slower than the 1.3 ms and 129 ms in the README. I measured the cause outside the engine: on this laptop the first read of a freshly written 20 KB file takes about 18 ms (creating it takes under 1 ms), and Windows real-time antivirus scanning is enabled. A full scan reads about 58 chunks of three files each, so it is consistent with that. I did not turn the scanner off to confirm, so this is a likely explanation, not a proven one. The stats-only aggregate reads no chunk files, which is why it's unaffected. The ingest row is also unaffected by reads.

## Writer scaling

```
cargo run --release --example concurrent_bench                  # default: wal_shards = 8
WAL_SHARDS=1 cargo run --release --example concurrent_bench     # a single log
```

N threads, one series each, 256-point batches, in M pts/s total. Three runs each.

| threads | `wal_shards` = 8 (default) | `wal_shards` = 1 |
|---:|---:|---:|
| 1 | 4.80 (4.57 – 5.19) | 4.63 (3.75 – 5.28) |
| 2 | 8.35 (8.25 – 8.81) | 3.84 (3.42 – 4.21) |
| 4 | 13.00 (11.72 – 14.88) | 3.99 (2.93 – 4.07) |
| 8 | 16.62 (15.92 – 17.93) | 3.81 (2.71 – 3.82) |

With a single log, adding writers makes things slower (from 4.6 M pts/s at one thread to 3.8 M at eight), which fits the log lock serialising them. With 8 shards, throughput climbs to about 3.5× the single-thread rate at 8 threads. This is the one comparison here I'd call solid, since the gap (4× at 8 threads) is far bigger than the run-to-run spread. The README quotes lower absolute numbers (7.5–8.3 M pts/s at 8 threads); today's are higher and I haven't investigated.

## Codecs

```
cargo bench --bench codecs -- --warm-up-time 1 --measurement-time 3
```

Criterion, 100,000 points, one run (its interval is the spread):

| | throughput |
|---|---:|
| timestamp encode / decode | 64.1 / 33.5 M pts/s |
| gorilla encode / decode | 8.9 / 16.3 M pts/s |
| `engine/append_batch` (WAL + active chunk) | 2.67 M pts/s |

## Queries, regex, delete

```
cargo run --release --example query_bench
```

100 series x 100,000 points (10 s apart, about 11 days), flushed to disk. One run, same laptop:

| | time |
|---|---:|
| ingest 10 M points + flush | 8.0 s |
| instant query, one series | 0.04 ms |
| `sum by (dc) (cpu)`, 2 days @ 10 min | 365 ms |
| `rate(cpu[5m])`, all series, 2 days @ 10 min | 146 ms |
| `topk(5, avg_over_time(cpu[1h]))` | 169 ms |
| `count(cpu{host=~"h0[0-4]."})` | 69 ms |
| delete one dc (25 series), 1 day: 216,000 points | 65 ms |

## Limits of these numbers

Single machine, a laptop whose clock speed moves with temperature, three runs per row. This file compares columnar-tsdb with no other engine.
