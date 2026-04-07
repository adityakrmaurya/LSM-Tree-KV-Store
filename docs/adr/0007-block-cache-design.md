# ADR-0007: Block Cache Design

## Status

Accepted

## Context

SSTable reads involve loading data blocks from disk. When the same blocks are accessed repeatedly
(e.g., hot keys, range scans over popular key ranges), re-reading from disk wastes I/O bandwidth
and increases latency.

A **block cache** keeps frequently accessed SSTable data blocks in memory, trading memory for
reduced disk I/O. Design choices:

| Approach | Pros | Cons |
|----------|------|------|
| No cache (rely on OS page cache) | Zero complexity | No control over eviction, memory usage |
| Per-SSTableReader cache | Simple | Redundant caching, hard to bound total memory |
| **Shared LRU cache** | Global memory bound, fair sharing | Requires thread-safe implementation |
| Clock/CLOCK-Pro | Lower overhead than LRU | More complex, marginal improvement |

## Decision

Use a **shared LRU block cache** for SSTable data blocks. Default capacity: **8 MB**.

### Design

```
Cache Key:   (sstableId: long, blockOffset: long)
Cache Value: byte[] (raw uncompressed block data)
Eviction:    LRU (Least Recently Used)
Scope:       One cache shared across ALL SSTableReaders in a DB instance
```

### What Gets Cached

- **Data blocks**: YES — these are the frequently accessed blocks containing key-value entries.
  Caching them avoids repeated disk reads.
- **Index blocks**: NO — index blocks are small (typically <1KB per SSTable) and are always held
  in memory by each `SSTableReader`. Caching them would waste cache space.
- **Filter blocks**: NO — Bloom filters are always held in memory by each `SSTableReader`.

### Implementation

Use `ConcurrentHashMap` for O(1) thread-safe lookups combined with an LRU eviction policy.

The access pattern is:
1. `SSTableReader.get(key)` determines which data block to read (via index block)
2. Check `blockCache.get(sstableId, blockOffset)`
3. Cache hit → return cached block (no disk I/O)
4. Cache miss → read block from disk, insert into cache, return

When an SSTable is deleted (after compaction), all its blocks are invalidated via
`blockCache.invalidate(sstableId)`.

### Sizing Rationale

- **8 MB default**: Conservative default suitable for most workloads. Holds ~2,000 data blocks
  (at 4KB each). Can be tuned up for read-heavy workloads with hot key ranges.
- **Configurable**: `DBConfig.blockCacheSize` allows tuning per deployment.
- **Rule of thumb**: Set cache size to hold the working set of hot blocks. For a database with
  10% hot data and 1GB total, a ~100MB cache would cache all hot blocks.

## Consequences

### Positive
- Reduces disk I/O for repeated reads of the same blocks
- Bounded memory usage (LRU eviction keeps total size under the configured limit)
- Shared across all SSTableReaders — automatically prioritizes the hottest blocks globally
- Thread-safe via `ConcurrentHashMap` — no lock contention on cache hits

### Negative
- LRU eviction is O(1) per access but requires maintaining a doubly-linked list alongside
  the hash map — adds implementation complexity
- 8 MB default may be too small for large databases (but is configurable)
- Cache invalidation on SSTable deletion requires scanning or tracking entries per SSTable

### Risks
- Scan-resistant workloads (sequential scans of cold data) can evict hot blocks from the cache.
  Mitigation: future enhancement could use a scan-resistant policy (LRU-K, ARC, or W-TinyLFU
  via the Caffeine library) or a `fillCache=false` option in `ReadOptions`.
- Memory fragmentation from varying block sizes. Mitigated by: blocks are close to the target
  4KB size, so fragmentation is minimal.

## References

- [LevelDB Cache](https://github.com/google/leveldb/blob/main/include/leveldb/cache.h) — shared LRU cache, 8MB default
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache) — LRU and Clock cache options
- [Caffeine Cache](https://github.com/ben-manes/caffeine) — W-TinyLFU eviction, potential future upgrade
