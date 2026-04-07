# ADR-0005: Concurrency Model

## Status

Accepted

## Context

A storage engine must handle concurrent reads and writes safely and efficiently. The concurrency
model affects correctness, performance, and implementation complexity. Key concerns:

1. **Multiple readers** should not block each other or block the writer
2. **Writes** must be serialized (WAL must have a total order of sequence numbers)
3. **Background tasks** (flush, compaction) must run concurrently with foreground operations
4. **MemTable rotation** (swapping active MemTable) must be atomic and safe

Options considered:

| Model | Pros | Cons |
|-------|------|------|
| **Coarse-grained locking** (single RWLock) | Simple | High contention, poor scalability |
| **Fine-grained locking** (per-component locks) | Better concurrency | Complex, deadlock risk |
| **Single writer + lock-free reads** | Simple write path, zero read contention | Write throughput limited to one thread |
| **Multi-writer** | Higher write throughput | Complex WAL ordering, MemTable sync overhead |

## Decision

Use **single writer + lock-free readers + virtual threads for background work**.

### Write Path: Single Writer

All writes are serialized through a single-writer pipeline:

```
[Thread 1] put(k1, v1) ──┐
[Thread 2] put(k2, v2) ──┼──→ WriteQueue ──→ [Single Writer Thread]
[Thread 3] delete(k3)  ──┘                         │
                                                    ├── WAL.append(batch)
                                                    ├── MemTable.put(entries)
                                                    └── return success to callers
```

**Why single writer:**
- The WAL requires a total ordering of writes (sequence numbers). A single writer trivially
  guarantees this without complex synchronization.
- LevelDB uses exactly this model. It's proven to work well for storage engines.
- Write throughput is bottlenecked by disk fsync latency, not CPU — a single thread can
  saturate the disk.
- The writer can batch multiple pending writes into a single WAL record and a single fsync
  (group commit), achieving better throughput than multiple writers each doing their own fsync.

### Read Path: Lock-Free

Reads never acquire any locks:

1. **MemTable reads**: `ConcurrentSkipListMap.get()` is lock-free (uses CAS-based traversal)
2. **SSTable reads**: SSTables are immutable files — concurrent reads are inherently safe
3. **Version access**: The current `Version` is accessed via an `AtomicReference` — reads see
   a consistent snapshot of which SSTables exist

### MemTable Rotation: StampedLock

When the MemTable is full, it must be frozen (made immutable) and replaced with a new empty
MemTable. This is the one operation that requires synchronization between readers and the writer:

```java
// Writer (rare — only during rotation):
long stamp = memTableLock.writeLock();
try {
    immutableMemTable = currentMemTable.freeze();
    currentMemTable = new MutableMemTable(config);
} finally {
    memTableLock.unlockWrite(stamp);
}

// Readers (hot path — every read):
long stamp = memTableLock.tryOptimisticRead();
MemTable current = this.currentMemTable;
MemTable immutable = this.immutableMemTable;
if (!memTableLock.validate(stamp)) {
    // Rare: rotation happened during read — fall back to read lock
    stamp = memTableLock.readLock();
    try {
        current = this.currentMemTable;
        immutable = this.immutableMemTable;
    } finally {
        memTableLock.unlockRead(stamp);
    }
}
// Now use current and immutable safely
```

**Why StampedLock:**
- Optimistic reads are **zero-cost** in the common case (no CAS, no memory barrier beyond a
  volatile read). Rotation happens maybe once every few seconds; the optimistic read succeeds
  >99.99% of the time.
- Much better than `ReentrantReadWriteLock` which has significant overhead even for read locks.

### Background Tasks: Virtual Threads

MemTable flushes and compaction run on virtual threads:

```java
Thread.ofVirtual().name("flush-", 0).start(() -> flushMemTable(immutable));
Thread.ofVirtual().name("compaction-", 0).start(() -> runCompaction(task));
```

**Why virtual threads:**
- Flush and compaction are I/O-bound (reading/writing SSTable files). Virtual threads are
  designed for I/O-bound work — they yield the platform thread during blocking I/O.
- Lightweight: millions of virtual threads can exist simultaneously (though we'll only have
  a handful). No need for a fixed thread pool.
- Simpler than configuring and tuning `Executors.newFixedThreadPool()`.

### Snapshot Isolation for Iterators

Iterators and snapshots reference a specific `Version` (set of SSTables). Versions are
reference-counted:

```
Iterator created → Version.retain()
Iterator closed  → Version.release()
Version refcount reaches 0 → obsolete SSTable files eligible for deletion
```

This ensures that compaction doesn't delete SSTable files that are still being read by
in-flight iterators.

## Consequences

### Positive
- Simple correctness: single writer eliminates WAL ordering bugs
- Zero read contention: lock-free MemTable + immutable SSTables
- Minimal synchronization overhead: StampedLock optimistic read is free in common case
- Virtual threads simplify background task management
- Group commit in single-writer pipeline improves throughput over naive per-write fsync

### Negative
- Write throughput is limited to what one thread can push through WAL + MemTable. For most
  workloads this is not the bottleneck (disk is), but CPU-bound write workloads may be limited.
- Single writer is a serialization point — high write concurrency sees queuing delay

### Risks
- If virtual thread scheduling has bugs or performance issues in the JVM, background tasks
  could be affected. Mitigated by: Java 21 is LTS and virtual threads are production-ready.
- StampedLock's optimistic read pattern requires careful implementation — a missed validation
  check leads to reading stale data. Mitigated by: encapsulating the pattern in a single method.

## References

- [LevelDB concurrency model](https://github.com/google/leveldb/blob/main/db/db_impl.cc) — single writer, concurrent readers
- [Java StampedLock Javadoc](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/locks/StampedLock.html)
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
