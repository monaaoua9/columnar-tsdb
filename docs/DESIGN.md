# columnar-tsdb: design decisions

Why the engine looks the way it does. Data flow, compression, query path, concurrency and recovery are described in the [README](../README.md).

## Scope

An embedded, zero-dependency time-series storage engine: Gorilla-style compression, immutable columnar chunks, a sharded WAL, a time-bucket index, label queries and downsampling, with sealing and compaction in a background thread.

On top of the engine, in the same crate and still with an empty dependency list: a PromQL-like query language, line-protocol ingest, deletes, online snapshots, rollups, a quantile sketch, a small HTTP server and a `tsdb` command-line tool.

## Decisions

**Immutable chunks, one file per column.** A sealed chunk is three files: timestamps (`.ts`, delta-of-delta), values (`.val`, Gorilla XOR) and `.meta` (stats plus the commit marker). A chunk only exists once its meta is in place, and compaction writes the merged chunk before swapping it in. Cost: many small files when buckets are tiny. With 2-hour buckets (5.5k chunks, 3 files each) an ingest run took about a minute on Windows because file creation dominated. Opening a database reads every `.meta`, so open time grows with the chunk count (about 0.35 ms per chunk on the dev laptop).

**Answer aggregates from chunk metadata.** Each chunk stores `count/sum/min/max`. A chunk that lies fully inside the query range contributes its stored stats and is never read from disk; only chunks straddling the range edges are decoded. That's why a full-range aggregate over a 500k-point series took 24 µs in the README run instead of a scan.

**Timestamps and values are compressed separately.** Delta-of-delta makes a fixed scrape interval cost one bit per point; XOR with the previous value reuses the leading/trailing-zero window when it fits. Both are exact for any `i64` / finite `f64` sequence. NaN and infinity are rejected at the door so the codec never needs a special case. There is no index inside a chunk: chunks hold at most 16,384 points, so a range read that decodes the timestamp column and then only the values it needs is cheap, and an in-chunk index would change the file format for very little.

**A WAL sharded by series id.** One log means one lock, and with a single log eight writer threads were flat at about 2.4M points/s. Sharding (default 8) spreads the lock. The price: `wal_shards` can't change without a clean `flush`, otherwise open refuses.

**One `RwLock` per series; readers decode with no lock held.** Queries copy a few `Arc`s and the filtered in-memory points under a read lock, then decode outside it. Freeze→seal is a single write-lock swap, so a reader sees every point exactly once. Old chunk files are deleted when the last `Arc` drops, so a running query never loses its files. Lock order: maintenance → catalog → series → WAL.

**Out-of-order writes are accepted, at a write-amplification cost.** A point newer than the series' newest timestamp takes the fast path. Anything else goes into a small sorted late buffer, is visible to queries immediately, and is folded into its bucket's chunk by the maintenance thread (decode, merge, rewrite one chunk). Same timestamp again means last write wins, even over a sealed value. Merging one late point rewrites its bucket, which is why the buffer batches them.

**A regex engine of our own, because the dependency list stays empty.** Label matchers have `=`, `!=`, `=~`, `!~` and a glob `=*`. `=~` is a Thompson-style NFA run as a set simulation, so matching is O(pattern × input) and a hostile pattern like `(a*)*b` can't blow up; the cost is no backreferences, lookaround or captures. Matching is whole-value, like PromQL. A matcher is resolved against the distinct values of one label in the in-memory index, never against points. Group nesting is capped at 100: the parser recurses, and a few thousand `(` used to overflow the stack (found by fuzzing).

**Deletes are logged as a range plus a chunk-id mark.** `delete_range` rewrites the sealed chunks that overlap the range and cuts the in-memory points. For crash safety the WAL gets two records written together: the range, then a *mark*, the chunk-id counter at that moment. On replay a delete is applied to memory and only to sealed chunks with an id below the mark. Without the mark, replay deleted points that were written *after* the delete, were already sealed before the crash, and only sit behind the delete record in the log; a randomized test with crash reopens found this, and a regression test fails without the filter. The active chunk is frozen when the delete happens so later writes get ids above the mark, the id counter is bumped past every mark on open, and the WAL is not truncated while a replaced chunk's `.meta` still exists (a running query can keep it alive), otherwise a crash could bring the old file back with no record left to delete from it again. A delete is all-or-nothing per series after a restart; `delete_select` writes one record per series, so it is not atomic across series. Old binaries must not open a WAL with delete records.

**The query language is a PromQL subset on the engine's own API.** Lexer, parser and evaluator, no external grammar. Selectors with the matchers above, range functions, `sum/avg/min/max/count ... by`, `topk`, scalar arithmetic and comparisons, and `offset`. Without a step a query is an instant value at `end`; with a step it is a range. `rate` is `increase / (last_ts - first_ts)` and extrapolates nothing. Comparing two scalars is an error. Parser recursion is capped at 100 levels. Fuzzing also found an endless loop in the lexer (`.count(a` allocated until the process died); it is a lexer bug, but reachable from `/query`, which is why the parsers are fuzzed.

**Line protocol: one f64 per field, and the field name is part of the series.** `value=` (or a bare number) keeps the metric name; any other field is appended with `_`, so `cpu usage=1,idle=2` writes `cpu_usage` and `cpu_idle`. Influx integers (`7i`) and booleans are accepted, strings are an error. An earlier version ignored the field name, which made two different fields at the same timestamp overwrite each other and kept only the last field of a multi-field line, silently; trying the tool on real data found it.

**The HTTP server is `std::net` and nothing else.** One thread per connection, 64 connections, 8 MiB bodies, `Connection: close`. There is no TLS because the standard library has none, and writing one would be worse than putting a proxy in front. An optional bearer token guards everything except `/health` (compared without bailing at the first differing byte), but it travels in clear text. `/delete` needs `match`, `start` and `end`, all three, so a forgotten parameter can never mean "everything".

**Snapshots hard-link sealed chunks.** Sealed chunks never change after their tmp+rename, so a snapshot links them and copies only the WAL and the catalog, under their locks, WAL first so every series a copied record names is in the copied catalog. No flush is forced: opening the copy replays the WAL. A forced flush used to make the snapshot wait behind a sealer that couldn't keep up. It still takes the maintenance lock to link files, and std mutexes aren't fair, so under a writer that outruns the sealer it can wait a long time.

**Rollups are ordinary series; quantiles are a DDSketch.** `cpu{host="a"}` rolled up at 5 minutes becomes `cpu:5m:avg{host="a"}`, `:max`, and so on, one point per completed window, so raw data can expire under `Config::retention` while long-range history stays queryable. The newest window is recomputed on every run, which makes re-running harmless. The sketch gives quantiles with a guaranteed relative error, mergeable, and small.

**The label index lives in memory and is rebuilt on open** from the series catalog. Simple and fast to query; the price is that open has to rebuild it.

## How it's checked

Randomized model tests (fixed seeds, no dependencies) compare against a `BTreeMap`: one mixes in-order, late and duplicate writes with flushes, compactions and crash-style reopens, another does the same with deletes (3 seeds × 250 steps). Concurrency tests assert readers see gap-free prefixes while sealing and compaction run. `tests/crash.rs` runs the writer as a separate process, kills it with `TerminateProcess` / `SIGKILL` at arbitrary moments on the same directory, reopens it in another process and checks every acknowledged point and nothing else; a second test does it with a delete every seventh step, including ranges that reach past the newest timestamp. The in-flight step is allowed to be missing, complete, or a batch without its delete.

`tests/fuzz.rs` mutates valid queries, lines and patterns (flip, drop, duplicate, insert bytes) and requires an answer, not a panic or a hang. The `load_test` and `soak` examples (20-100 M points; 18 days of data time with unclean reopens every 20 s) check exact point counts; their numbers and caveats are in [BENCHMARKS.md](BENCHMARKS.md).

Things this found that unit tests had not: the delete replay bug above (randomized test), the lexer loop and two stack overflows (fuzzing), and the line-protocol field handling (using it on real data).

## Known limits

A batch is validated atomically but not made durable atomically (after a crash an in-flight batch may survive as a prefix; acknowledged batches are never lost). `delete_select` is not atomic across series. The block cache evicts FIFO, not LRU. WAL records are 29 bytes uncompressed. Sum is a plain f64 accumulation. No fsync of the chunk directory after renames. No TLS, no replication, one process per directory. Open time grows with the number of chunks. The README's Limits section is the complete list.
