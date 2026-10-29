# columnar-tsdb: design decisions

Why the engine looks the way it does. Data flow, compression, query path, concurrency and recovery are described in the [README](../README.md).

## Scope

An embedded, zero-dependency time-series storage engine: Gorilla-style compression, immutable columnar chunks, a sharded WAL, a time-bucket index, label queries and downsampling, with sealing and compaction in a background thread.

## Decisions

**Immutable chunks, one file per column.** A sealed chunk is three files: timestamps (`.ts`, delta-of-delta), values (`.val`, Gorilla XOR) and `.meta` (stats plus the commit marker). A chunk only exists once its meta is in place, and compaction writes the merged chunk before swapping it in. Cost: many small files when buckets are tiny. With 2-hour buckets (5.5k chunks, 3 files each) an ingest run took about a minute on Windows because file creation dominated.

**Answer aggregates from chunk metadata.** Each chunk stores `count/sum/min/max`. A chunk that lies fully inside the query range contributes its stored stats and is never read from disk; only chunks straddling the range edges are decoded. That's why a full-range aggregate over a 500k-point series took 24 µs in the README run instead of a scan.

**Timestamps and values are compressed separately.** Delta-of-delta makes a fixed scrape interval cost one bit per point; XOR with the previous value reuses the leading/trailing-zero window when it fits. Both are exact for any `i64` / finite `f64` sequence. NaN and infinity are rejected at the door so the codec never needs a special case.

**A WAL sharded by series id.** One log means one lock, and with a single log eight writer threads were flat at about 2.4M points/s. Sharding (default 8) spreads the lock. The price: `wal_shards` can't change without a clean `flush`, otherwise open refuses.

**One `RwLock` per series; readers decode with no lock held.** Queries copy a few `Arc`s and the filtered in-memory points under a read lock, then decode outside it. Freeze→seal is a single write-lock swap, so a reader sees every point exactly once. Old chunk files are deleted when the last `Arc` drops, so a running query never loses its files. Lock order: maintenance → catalog → series → WAL.

**Out-of-order writes are accepted, at a write-amplification cost.** A point newer than the series' newest timestamp takes the fast path. Anything else goes into a small sorted late buffer, is visible to queries immediately, and is folded into its bucket's chunk by the maintenance thread (decode, merge, rewrite one chunk). Same timestamp again means last write wins, even over a sealed value. Merging one late point rewrites its bucket, which is why the buffer batches them.

**Zero dependencies means no regex.** Label matching has `eq`, `ne` and a glob (`*`, `?`) that walks only the distinct values of one label. A full regex engine wasn't worth giving up the empty dependency list.

**The label index lives in memory and is rebuilt on open** from the series catalog. Simple and fast to query; the price is that open has to rebuild it.

## How it's checked

A randomized model test mixes in-order, late and duplicate writes with flushes, compactions and crash-style reopens against a `BTreeMap`. Concurrency tests assert readers see gap-free prefixes while sealing and compaction run. `tests/crash.rs` runs the writer as a separate process, kills it with `TerminateProcess` / `SIGKILL` at ten moments on the same directory, reopens it in another process and checks every acknowledged point and nothing else.

## Known limits

A batch is validated atomically but not made durable atomically (after a crash an in-flight batch may survive as a prefix; acknowledged batches are never lost). The block cache evicts FIFO, not LRU. WAL records are 29 bytes uncompressed. Sum is a plain f64 accumulation. No fsync of the chunk directory after renames.
