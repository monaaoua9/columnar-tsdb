# columnar-tsdb: benchmarks

Measured 2026-10-29 (ingest, writer scaling, codecs) and 2026-10-30/31 (queries, bigger data, soak).

| | |
|---|---|
| CPU | Intel i5-8365U, 4 cores / 8 threads, 1.6 GHz base |
| RAM / disk | 8 GB, SSD, C: drive nearly full |
| OS | Windows 11, real-time antivirus on |
| Conditions | about six terminals open at the same time; timings swing 2-5x between runs |
| Toolchain | Rust 1.99.1, `--release` |

## How much to trust each section

| section | what it is | runs | trust it for |
|---|---|---|---|
| Ingest and queries | ingest throughput, aggregates, one scan | 3 | order of magnitude; the two read rows are mostly the OS (see below) |
| Writer scaling | 1 vs 8 WAL shards, 1-8 threads | 3 | the *comparison*: the gap is far bigger than the spread |
| Codecs | criterion micro benchmarks | 1 (with its interval) | CPU-bound, the least noisy numbers here |
| Queries, regex, delete | evaluator over 10 M points | 1 | order of magnitude only |
| Bigger data | 20 M and 100 M points, reopen, size | 2 and 1 | the sizes (2.88 B/point) and "nothing lost"; times are rough |
| Soak | 4 min of continuous ingest, reads and unclean reopens | 1 | "no loss, no failed query, no visible leak"; not for throughput |

No section compares columnar-tsdb with another engine. Absolute numbers from a machine like this are rough; a quieter or faster one will mostly just be faster.

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

100 series x 100,000 points (10 s apart, about 11 days), flushed to disk. One run on the busy laptop described at the top:

| | time |
|---|---:|
| ingest 10 M points + flush | 8.0 s |
| instant query, one series | 0.04 ms |
| `sum by (dc) (cpu)`, 2 days @ 10 min | 365 ms |
| `rate(cpu[5m])`, all series, 2 days @ 10 min | 146 ms |
| `topk(5, avg_over_time(cpu[1h]))` | 169 ms |
| `count(cpu{host=~"h0[0-4]."})` | 69 ms |
| delete one dc (25 series), 1 day: 216,000 points | 65 ms |

## Bigger data

```
cargo run --release --example load_test [series] [points_per_series]
```

Ingests, drops the engine without a flush and reopens it (wal replay), checks that every point is still there, flushes, compacts, reopens again. Points are 10 s apart, one chunk per series per day.

| | 50 x 400 k = 20 M points (two runs) | 100 x 1 M = 100 M points (one run) |
|---|---:|---:|
| ingest | 23.5 s / 20.3 s | 139 s |
| reopen without flush | 1.28 s / 0.97 s | 7.3 s |
| reopen after flush + compact | 1.07 s / 0.73 s | 5.0 s |
| decode one whole series | 70 ms / 50 ms | 317 ms |
| on disk | 57.6 MB (2.88 B/point) | 287.8 MB (2.88 B/point) |

No point was lost in either size. Reopen time grows with the number of chunk files it has to open (about 11,600 in the big run), a bit faster than linearly: 5x the data took about 5-7x as long. Peak working set of the big run was 31 MB, but that was sampled by polling every few seconds from outside, so it is a floor, not a measurement. Wall times come from the busy laptop described at the top.

## Soak

```
cargo run --release --example soak [seconds] [series]
```

A collector's life squeezed into minutes: 200 series, one point per series per tick, 10 s of data time per tick, one single-point `append_batch` per series (the worst case for the log), plus a reader running `avg by (host) (rate(sys{m="cpu"}[5m]))` every 200 ms. Every 20 s the engine is dropped without a flush and reopened, and the point count has to match what was written. Retention is off so the count is exact.

One 240 s run on the busy laptop described at the top: 31.6 M points (18.3 days of data time), 12 reopens, every count exact, 1,007 queries and none failed, no maintenance error.

| | start | end |
|---|---:|---:|
| sealed chunks | 1,317 (at 20 s) | 3,773 |
| reopen time | 0.59 s | 1.34 s |
| WAL size | 96 MB | 72 MB |

The WAL stays between about 66 and 97 MB for the whole run. Chunk count grows linearly with data time (about one chunk per series per day, so compaction is keeping up) and reopen time with it, about 0.35 ms per chunk, the same figure as in the load test. Up to 165 chunks were frozen and waiting for the sealer at the end of a 20 s stretch: the writer outruns the sealer here, and backpressure is what holds it. Working set, polled from outside every ~15 s, stayed between 15 and 49 MB, with no upward trend; the drops are the reopens. That polling is coarse, so read it as "no leak visible", not as a peak.

The writer here does about 130,000 points/s (31.6 M points in 240 s, reopens and the reader included in that wall time) because every point is its own `append_batch`; the same engine takes millions per second in batches (see Codecs and Writer scaling). Do not compare those numbers.

This is 4 minutes, not days. It says nothing about a disk filling up or about retention under load.

## Limits of these numbers

Single machine, a laptop whose clock speed moves with temperature, with other work running on it (see the table at the top). Sections with one run say so; the others have three. When comparing two builds, run them interleaved in one session and read best-of or the median, not a single run.
