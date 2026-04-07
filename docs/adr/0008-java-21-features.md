# ADR-0008: Java 21 Modern Features

## Status

Accepted

## Context

Java 21 is the latest LTS (Long-Term Support) release and introduces several features that
reduce boilerplate, improve type safety, and enable more efficient concurrency. Since this
is a greenfield project with no legacy constraints, we can fully embrace modern Java.

Key Java 21 features relevant to a storage engine:

| Feature | JEP | Available Since |
|---------|-----|-----------------|
| Records | [JEP 395](https://openjdk.org/jeps/395) | Java 16 |
| Sealed Classes | [JEP 409](https://openjdk.org/jeps/409) | Java 17 |
| Pattern Matching for instanceof | [JEP 394](https://openjdk.org/jeps/394) | Java 16 |
| Pattern Matching for switch | [JEP 441](https://openjdk.org/jeps/441) | Java 21 |
| Virtual Threads | [JEP 444](https://openjdk.org/jeps/444) | Java 21 |
| Sequenced Collections | [JEP 431](https://openjdk.org/jeps/431) | Java 21 |

## Decision

Adopt **all applicable Java 21 features** throughout the codebase. This is not optional — every
module must use modern Java idioms where they apply.

### Records — for all immutable data carriers

```java
// Instead of verbose POJO classes with equals/hashCode/toString:
public record BlockHandle(long offset, long size) {}
public record Footer(BlockHandle metaIndexHandle, BlockHandle indexHandle, long magicNumber) {}
public record InternalEntry(byte[] key, byte[] value, long sequenceNumber, EntryType type) {}
public record CompactionTask(int inputLevel, List<SSTableMetadata> inputs, List<SSTableMetadata> grandparents) {}
public record DBConfig(Path dbPath, long maxMemTableSize, /* ... */) {}
```

**Why**: Records eliminate 50+ lines of boilerplate per data class (constructor, getters,
equals, hashCode, toString). They also signal immutability clearly — a record's fields are
final by definition.

**Where to use**: Any class whose primary purpose is to carry data: configuration objects,
internal entries, metadata, handles, edits.

**Where NOT to use**: Mutable stateful classes (SSTableWriter, WALWriter, BlockCache).

### Sealed Interfaces — for closed type hierarchies

```java
public sealed interface MemTable permits MutableMemTable, ImmutableMemTable {}
public sealed interface CompactionStrategy permits LeveledCompaction {}
public sealed interface FilterPolicy permits BloomFilter {}
public sealed interface InternalIterator permits MemTableIterator, SSTableIterator, MergingIterator {}
```

**Why**: Sealed interfaces give the compiler a complete list of subtypes, enabling:
- Exhaustive `switch` expressions (compiler error if a case is missing)
- Clear documentation of the type hierarchy
- Prevention of unauthorized implementations (the `permits` clause is enforced)

### Pattern Matching — for type-safe dispatch

```java
// Pattern matching for switch (Java 21):
return switch (entry.type()) {
    case PUT -> processWrite(entry);
    case DELETE -> processTombstone(entry);
};

// Pattern matching for instanceof:
if (e instanceof StorageException se) {
    logger.error("I/O failure on file {}: {}", se.path(), se.getMessage());
}
```

**Why**: Eliminates manual casting and makes type dispatch exhaustive.

### Virtual Threads — for background I/O-bound tasks

```java
// MemTable flush:
Thread.ofVirtual().name("flush-", flushCount).start(() -> flushMemTable(immutable));

// Compaction:
Thread.ofVirtual().name("compaction-L", level).start(() -> runCompaction(task));
```

**Why**: Flush and compaction are I/O-bound (reading/writing files). Virtual threads:
- Yield the platform thread during blocking I/O (FileChannel read/write)
- Are extremely lightweight (a few hundred bytes each vs ~1MB per platform thread)
- Don't require thread pool sizing or configuration
- Simplify the code (no `ExecutorService`, `Future`, or callback chains)

**Where NOT to use**: CPU-bound work (e.g., compression, CRC computation). These should run
on platform threads to avoid virtual thread pinning.

### Sequenced Collections — for ordered iteration

```java
// VersionSet maintains ordered SSTables:
SequencedMap<Long, SSTableMetadata> l0Files; // ordered by file number (newest first)
```

**Why**: `SequencedCollection` and `SequencedMap` make ordered semantics explicit in the type
system. `getFirst()`, `getLast()`, and `reversed()` replace ad-hoc index-based access patterns.

## Consequences

### Positive
- Dramatically reduced boilerplate (~40% less code for data classes)
- Compile-time exhaustiveness checking for sealed hierarchies and pattern matching
- Clearer code intent: records signal "data carrier", sealed signals "closed hierarchy"
- Virtual threads simplify background task management without sacrificing performance
- Modern Java attracts contributors familiar with current best practices

### Negative
- Java 21 is the minimum required JVM version — cannot run on Java 17 or earlier
- Records with `byte[]` fields require custom `equals`/`hashCode` (arrays use reference equality).
  Mitigated by: wrapper utility methods for comparison, or accepting reference equality where appropriate.
- Virtual threads have limited support for `synchronized` blocks (pinning risk). Must use
  `ReentrantLock` or `StampedLock` instead of `synchronized` in virtual thread contexts.

### Risks
- Some IDEs and tools may have incomplete support for newer Java features. Mitigated by: Java 21
  has been available since September 2023 and has broad tooling support.
- Virtual thread pinning when calling `synchronized` native methods (e.g., in some NIO paths).
  Mitigated by: using `FileChannel` (which is virtual-thread-friendly) and avoiding `synchronized`.

## References

- [JEP 395: Records](https://openjdk.org/jeps/395)
- [JEP 409: Sealed Classes](https://openjdk.org/jeps/409)
- [JEP 441: Pattern Matching for switch](https://openjdk.org/jeps/441)
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
- [JEP 431: Sequenced Collections](https://openjdk.org/jeps/431)
