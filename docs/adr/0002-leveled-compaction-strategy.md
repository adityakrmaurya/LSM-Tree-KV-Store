# ADR-0002: Leveled Compaction Strategy

## Status

Accepted

## Context

As the MemTable fills and flushes to disk, SSTables accumulate. Without compaction, read
performance degrades because every point lookup may need to check many overlapping files.
Compaction merges SSTables to reduce redundancy and maintain read efficiency.

The two dominant strategies are:

| Strategy | Write Amp | Read Amp | Space Amp | Used By |
|----------|-----------|----------|-----------|---------|
| **Size-Tiered** | Low (~5-10x) | High (many files per lookup) | High (~2x) | Cassandra (default) |
| **Leveled** | High (~10-30x) | Low (1 file per level per lookup) | Low (~1.1x) | LevelDB, RocksDB (default) |

**Write amplification** = total bytes written to disk / bytes written by user.
**Read amplification** = number of SSTables checked per point lookup.
**Space amplification** = total disk usage / actual data size.

## Decision

Use **leveled compaction** as the compaction strategy.

### How it works

1. **Level 0 (L0)**: Freshly flushed SSTables land here. L0 files may have overlapping key ranges.
   When L0 accumulates more than `l0CompactionTrigger` (default: 4) files, compact L0 → L1.

2. **Level 1+ (L1..Ln)**: Each level has a max total size target. L1 = 10MB, L2 = 100MB, L3 = 1GB,
   etc. (each level is `levelSizeMultiplier` × the previous, default: 10x). Within a level,
   SSTables have **non-overlapping key ranges** — at most one file contains any given key.

3. **Compaction trigger**: When level Ln's total size exceeds its target, pick one file from Ln
   (round-robin to spread I/O evenly), find all overlapping files in Ln+1, merge them, and write
   the output back to Ln+1.

4. **L0 → L1 special case**: Since L0 files may overlap each other, L0→L1 compaction takes ALL
   overlapping L0 files plus all overlapping L1 files and merges them together.

### Why leveled over size-tiered

- **Read performance**: For L1+, a point lookup checks at most **one SSTable per level** (since
  files don't overlap within a level). With 7 levels, that's at most ~11 SSTable probes
  (4 L0 + 1 per L1-L6), each filtered by a Bloom filter (~1% FPR). Expected disk reads per
  lookup: ~0.11.

- **Space amplification**: Only ~10% overhead, because compaction eagerly removes obsolete
  versions and tombstones. This matters when storage cost is a concern.

- **Predictable behavior**: Level sizes grow geometrically, so disk usage is predictable and
  bounded. No surprise spikes in space usage.

## Consequences

### Positive
- Excellent point lookup performance (bounded number of files to check per level)
- Bounded, predictable space amplification (~1.1x)
- Non-overlapping files in L1+ simplify range scans (less merging needed)
- Well-understood algorithm with extensive production validation (LevelDB, RocksDB)

### Negative
- Higher write amplification than size-tiered (~10-30x). Each byte written by the user may be
  rewritten 10-30 times as it gets compacted through levels. This increases disk I/O and SSD wear.
- More complex implementation than size-tiered (must track per-level invariants, handle L0
  overlap, manage round-robin compaction pointers)
- Background compaction competes with foreground I/O, requiring careful scheduling

### Risks
- Write-heavy workloads with large values may suffer from high write amplification. Mitigated
  by: (a) tuning `levelSizeMultiplier` (lower = less write amp, more levels), (b) future option
  to add size-tiered as an alternative strategy behind the `CompactionStrategy` interface.
- L0→L1 compaction can be expensive if L0 accumulates many files. Mitigated by `l0SlowdownTrigger`
  and `l0StopTrigger` back-pressure mechanisms.

## References

- [LevelDB Compaction](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction)
- O'Neil, P. et al. (1996). "The Log-Structured Merge-Tree (LSM-Tree)"
- Luo, C. & Carey, M.J. (2020). "LSM-based Storage Techniques: A Survey"
