# ADR-0001: Use Skip List for MemTable

## Status

Accepted

## Context

The MemTable is the in-memory write buffer of the LSM Tree. Every write goes to the MemTable
first, and every read checks the MemTable before going to disk. The MemTable must support:

1. **Sorted insertion** — keys must be maintained in sorted order for efficient flush to SSTables
2. **Concurrent access** — multiple reader threads must not block writers (and vice versa)
3. **Range iteration** — range scans require iterating entries in sorted order
4. **Good performance** — O(log n) insert and lookup

The main candidates are:

| Data Structure | Concurrency | Complexity | Used By |
|---------------|-------------|------------|---------|
| **Skip List** | Lock-free reads (CAS) | O(log n) avg | LevelDB, RocksDB |
| **Red-Black Tree** | Requires external lock | O(log n) worst | — |
| **AVL Tree** | Requires external lock | O(log n) worst | — |
| **B-Tree** | Complex concurrent variants | O(log n) worst | WiredTiger |

## Decision

Use `java.util.concurrent.ConcurrentSkipListMap` as the backing data structure for the MemTable.

**Why ConcurrentSkipListMap specifically:**

1. **Lock-free reads** — Read operations use optimistic traversal without acquiring any locks.
   This is critical because reads vastly outnumber writes in most workloads, and read latency
   directly impacts user-facing query performance.

2. **Battle-tested implementation** — Java's standard library implementation has been in production
   across millions of JVMs since Java 6. No need to implement our own concurrent sorted structure.

3. **Natural sorted iteration** — `ConcurrentSkipListMap` implements `NavigableMap`, providing
   `subMap()`, `headMap()`, `tailMap()` for efficient range scans without additional code.

4. **Same choice as LevelDB/RocksDB** — Both use skip lists for their MemTables. This is a proven
   pattern in the storage engine space.

5. **Probabilistic O(log n)** — While technically average-case (not worst-case like balanced trees),
   the constant factors are excellent and the probability of pathological behavior is astronomically
   low with standard random level generation.

## Consequences

### Positive
- Lock-free reads enable high concurrent read throughput without contention
- Zero implementation effort — `ConcurrentSkipListMap` is in the JDK
- `NavigableMap` API provides range scan primitives for free
- Follows proven patterns from LevelDB/RocksDB

### Negative
- Higher memory overhead than a red-black tree (skip list nodes have variable-size forward pointer
  arrays, averaging ~1.33 pointers per node vs exactly 3 for red-black)
- O(log n) is average-case, not worst-case (though pathological cases are vanishingly unlikely)
- No control over skip list internals (level probability, max level) since we use the JDK implementation

### Risks
- If we later need custom memory accounting (e.g., off-heap MemTable), we'd need to replace or
  wrap `ConcurrentSkipListMap`. Mitigated by the `MemTable` sealed interface — callers don't
  depend on the concrete implementation.

## References

- [LevelDB SkipList implementation](https://github.com/google/leveldb/blob/main/db/skiplist.h)
- [RocksDB InlineSkipList](https://github.com/facebook/rocksdb/blob/main/memtable/inlineskiplist.h)
- [Java ConcurrentSkipListMap Javadoc](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ConcurrentSkipListMap.html)
- Pugh, W. (1990). "Skip Lists: A Probabilistic Alternative to Balanced Trees"
