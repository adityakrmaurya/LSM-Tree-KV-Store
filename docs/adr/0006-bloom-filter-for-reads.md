# ADR-0006: Bloom Filters for Read Optimization

## Status

Accepted

## Context

In an LSM Tree, a point lookup (`get(key)`) may need to probe multiple SSTables across multiple
levels. Without optimization, a lookup for an absent key checks every SSTable — the worst case.

For a database with 7 levels and 4 L0 files, that's up to ~10 SSTable probes per lookup, each
involving a disk read. Since most probes return "not found" (especially for absent keys or
keys at deeper levels), we need a way to skip SSTables that definitely don't contain the key.

A **Bloom filter** is a space-efficient probabilistic data structure that answers the question
"might this set contain element X?" with:
- **Definite NO** (100% accurate) — the key is definitely not in this SSTable, skip it
- **Probable YES** (with false positive rate ε) — the key might be here, must check

## Decision

Use **per-SSTable Bloom filters** with a default configuration of **10 bits per key**.

### Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Bits per key | 10 | ~1% false positive rate. Sweet spot between memory and accuracy. |
| Hash functions (k) | 7 | Optimal k for 10 bits/key: k = (m/n) × ln(2) ≈ 6.93 |
| Hash algorithm | Double hashing from single Murmur3 | Two 32-bit halves of Murmur3-128 → h(i) = h1 + i×h2. Avoids computing k independent hashes. |

### False Positive Rate Analysis

With 10 bits per key and k=7 hash functions:
- **Theoretical FPR**: (1 - e^(-7/10))^7 ≈ 0.82% ≈ **1%**
- **Practical impact**: For a lookup that probes 10 SSTables (worst case), expected unnecessary
  disk reads due to false positives: 10 × 0.01 = 0.1. That is, on average, only 1 in 10
  lookups incurs a single unnecessary disk read.

### Memory Cost

- **Per key**: 10 bits = 1.25 bytes
- **1 million keys**: 1.25 MB
- **100 million keys**: 125 MB (may require lazy loading for deeper levels)
- Filters are small relative to the data they protect and are always held in memory

### Lifecycle

1. **Built during SSTable creation**: As `SSTableWriter.add(key, value)` is called, each key
   is also inserted into the Bloom filter.
2. **Serialized into the SSTable**: The filter is written as the Filter Block in the SSTable file.
3. **Loaded on SSTable open**: `SSTableReader.open()` reads the Filter Block and deserializes
   the Bloom filter into memory.
4. **Queried on every point lookup**: Before reading any data block, `bloomFilter.mightContain(key)`
   is checked. If false, the SSTable is skipped entirely.

### Implementation Notes

- Use double hashing (Kirsch-Mitzenmacker optimization): compute two hashes h1, h2 from a
  single Murmur3-128 call, then derive k probes as `h1 + i * h2` for i in 0..k-1.
  This is provably equivalent to k independent hash functions for Bloom filters.
- The `FilterPolicy` sealed interface allows future alternative implementations (e.g., Ribbon
  filters, Xor filters) without changing the SSTable format.

## Consequences

### Positive
- Eliminates ~99% of unnecessary SSTable reads for absent keys
- Dramatically improves average point lookup latency (especially for negative lookups)
- Small memory footprint (1.25 bytes per key)
- Simple implementation with well-understood mathematical properties
- Pluggable via `FilterPolicy` interface for future improvements

### Negative
- ~1% false positive rate means occasional unnecessary disk reads
- Memory grows linearly with total key count across all SSTables
- Bloom filters don't help with range scans (only point lookups)
- Additional CPU cost per write (insert into filter) and read (probe filter), though negligible

### Risks
- For very large databases (billions of keys), Bloom filter memory could become significant.
  Mitigation: lazy-load filters for deeper levels (L3+) or use more space-efficient structures
  like Ribbon filters.
- Poor hash function choice leads to higher-than-expected FPR. Mitigated by using Murmur3,
  which has excellent avalanche properties.

## References

- Bloom, B.H. (1970). "Space/Time Trade-offs in Hash Coding with Allowable Errors"
- Kirsch, A. & Mitzenmacker, M. (2006). "Less Hashing, Same Performance: Building a Better Bloom Filter"
- [LevelDB FilterPolicy](https://github.com/google/leveldb/blob/main/include/leveldb/filter_policy.h)
- [RocksDB Bloom Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)
