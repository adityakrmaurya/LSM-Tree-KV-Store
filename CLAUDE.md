# CLAUDE.md — LSM Tree KV Store

## Git Identity

All commits MUST use the following git identity:
- **user.name**: `adityakrmaurya`
- **user.email**: `adityakrmaurya03@gmail.com`

## Project Overview

Production-grade LSM Tree (Log-Structured Merge Tree) Key-Value Store in Java 21.
Inspired by LevelDB and RocksDB. Apache 2.0 License.

## Build & Test Commands

```bash
./gradlew build                    # Compile + test + checkstyle + spotless
./gradlew test                     # Run all tests
./gradlew spotlessCheck            # Verify Google Java Format compliance
./gradlew spotlessApply            # Auto-format all Java files
./gradlew checkstyleMain           # Run checkstyle on main sources
./gradlew checkstyleTest           # Run checkstyle on test sources
./gradlew jacocoTestReport         # Generate code coverage report
./gradlew test --tests "com.lsmtreestore.memtable.MutableMemTableTest"  # Single test class
```

## Code Style & Conventions

### Google Java Style (STRICTLY ENFORCED)

Enforced via Spotless (Google Java Format 1.24.0) and Checkstyle (Google Checks 10.20.1).

- **2-space indentation** (NO tabs)
- **100-character line limit**
- **K&R braces** (opening brace on same line)
- **No wildcard imports** — always use explicit imports
- **Static imports separated** from regular imports
- **One top-level class per file**
- Reference: https://google.github.io/styleguide/javaguide.html

### Java 21 Conventions

- Use `record` for all immutable data carriers (Entry, BlockHandle, Config, etc.)
- Use `sealed interface` + `permits` for closed type hierarchies
- Use pattern matching (`instanceof`, `switch`) instead of manual casting
- Use virtual threads (`Thread.ofVirtual()`) for I/O-bound background tasks (compaction, flush)
- Use `try-with-resources` for ALL `Closeable`/`AutoCloseable` resources
- Prefer `Optional<V>` return types over null for query/lookup methods
- **NEVER** return null from a public method — use `Optional` or throw an exception
- Use `SequencedCollection`/`SequencedMap` where insertion order matters

### Naming Conventions

| Element       | Convention               | Example                        |
|---------------|--------------------------|--------------------------------|
| Package       | `com.lsmtreestore.<mod>` | `com.lsmtreestore.memtable`    |
| Class         | PascalCase, nouns        | `SSTableWriter`, `BloomFilter` |
| Method        | camelCase, verb phrases  | `writeBlock()`, `flushToDisk()`|
| Constant      | UPPER_SNAKE_CASE         | `MAX_MEMTABLE_SIZE`            |
| Local var     | camelCase                | `blockOffset`                  |
| Type param    | Single uppercase letter  | `<K>`, `<V>`                   |
| Test class    | `<ClassName>Test`        | `MutableMemTableTest`          |
| Test method   | `method_condition_expected` | `get_existingKey_returnsValue` |

### Error Handling

- Use specific exception types — never catch/throw raw `Exception`
- Custom exception hierarchy:
  - `StoreException` — base (extends `RuntimeException`)
  - `StorageException` — I/O and file system failures
  - `CorruptionException` — data integrity violations (bad CRC, truncated file)
- Log at appropriate levels:
  - `ERROR` — unrecoverable failures
  - `WARN` — degraded operation (e.g., slow compaction)
  - `INFO` — lifecycle events (open, close, flush, compaction start/end)
  - `DEBUG` — internal details (block reads, cache hits/misses)
- Use **SLF4J** for all logging — never `System.out` or `System.err`

### Documentation

- **All public classes and methods MUST have Javadoc**
- Javadoc first sentence: concise summary ending with a period
- Include `@param`, `@return`, `@throws` for all public methods
- Use non-Javadoc comments (`//` or `/* */`) for implementation notes
- For every design decision (data structure, algorithm, pattern), briefly document **WHY** in a comment

### Testing

- **JUnit 5** with **AssertJ** assertions (not Hamcrest)
- Test naming: `methodName_condition_expectedResult`
  - Example: `put_nullKey_throwsNullPointerException`
- **One assertion concept per test method** (multiple related asserts are fine)
- Use `@TempDir` for all file-based tests — never hardcode paths
- Use `@Nested` classes to group related tests within a test class
- Target: **90%+ line coverage** for core engine packages
- Write both **unit tests** and **integration tests** where appropriate

## Architecture Overview

### Write Path
```
Client.put(key, value)
  → WriteBatch (may group multiple ops)
  → Acquire write queue slot (single writer serialization)
  → WALWriter.append(batch)         // Sequential disk write
  → fsync WAL (if sync=true, else group commit)
  → MemTable.put(key, value, seqNo++)
  → If MemTable size > threshold (4MB):
      → Freeze → ImmutableMemTable
      → Create new MutableMemTable
      → Schedule background flush (virtual thread)
  → Return success
```

### Read Path
```
Client.get(key)
  → Acquire current Version (reference counted)
  → Search MutableMemTable → found? return
  → Search ImmutableMemTable (if exists) → found? return
  → For each level L0..Ln:
      L0: Check ALL files (newest first, may overlap)
        → BloomFilter check → if maybe-present, search SSTable
      L1+: Binary search for ONE file whose key range contains key
        → BloomFilter check → if maybe-present, search SSTable
  → Not found → return Optional.empty()
```

### Flush Path
```
ImmutableMemTable
  → Iterate all entries in sorted order
  → SSTableWriter builds L0 SSTable (data blocks, index, bloom filter, footer)
  → VersionEdit: add new SSTable to L0
  → Apply to VersionSet (atomic pointer swap)
  → Persist to MANIFEST
  → Delete old WAL
```

### Compaction Path (Leveled)
```
CompactionScheduler detects trigger:
  → L0 files > 4: trigger L0→L1
  → Ln size > 10^n × 10MB: trigger Ln→Ln+1
LeveledCompaction picks input files + overlapping files in output level
  → MergingIterator merges all inputs in sorted order
  → Drop tombstones older than oldest snapshot
  → Drop superseded versions of same key
  → Write output SSTables (rotate at target size)
  → VersionEdit: remove inputs, add outputs
  → Apply atomically, delete obsolete files
```

### Recovery
```
On open:
  → Read MANIFEST to reconstruct VersionSet (which SSTables exist per level)
  → Replay WAL entries not yet flushed to SSTables
  → Resume normal operation
```

## Package Structure

```
com.lsmtreestore
├── api/          # Public KVStore interface, ReadOptions, WriteOptions, WriteBatch, Snapshot
├── common/       # Byte utilities, Coding (varint), Checksum, exception hierarchy
├── config/       # DBConfig record with all tunable parameters and defaults
├── wal/          # WALWriter, WALReader, WALRecord, WALRecordType
├── memtable/     # MemTable (sealed), MutableMemTable, ImmutableMemTable, InternalEntry
├── sstable/      # SSTableWriter/Reader, block/ (Block, BlockBuilder, BlockCache),
│                 # filter/ (BloomFilter, FilterPolicy), Footer, IndexBlock
├── compaction/   # CompactionStrategy (sealed), LeveledCompaction, CompactionTask/Worker/Scheduler
├── version/      # Version, VersionSet, VersionEdit, ManifestWriter
├── iterator/     # InternalIterator (sealed), MemTableIterator, SSTableIterator,
│                 # MergingIterator, DBIterator
└── engine/       # DBImpl (ties everything together), WriteQueue, Recovery
```

## Module Dependency Order (build bottom-up)

1. `common` — byte utilities, config, exceptions (zero dependencies)
2. `config` — DBConfig record (depends on common)
3. `wal` — write-ahead log (depends on common)
4. `memtable` — skip list MemTable (depends on common, config)
5. `sstable` — SSTable reader/writer, bloom filter, block cache (depends on common, config)
6. `iterator` — iterator abstractions (depends on memtable, sstable)
7. `version` — version management, MANIFEST (depends on sstable)
8. `compaction` — leveled compaction (depends on sstable, version, iterator)
9. `engine` — DBImpl, recovery (depends on all above)
10. `api` — public KVStore interface (depends on engine)

## Key Design Decisions

| Decision | Choice | Rationale | ADR |
|----------|--------|-----------|-----|
| MemTable DS | ConcurrentSkipListMap | Lock-free reads, same as LevelDB/RocksDB | [ADR-0001](docs/adr/0001-use-skip-list-for-memtable.md) |
| Compaction | Leveled | Bounded space amp (~1.1x), better read perf | [ADR-0002](docs/adr/0002-leveled-compaction-strategy.md) |
| SSTable format | LevelDB-inspired | Data blocks + index + bloom + footer, proven design | [ADR-0003](docs/adr/0003-sstable-file-format.md) |
| WAL format | CRC32 + 32KB blocks | Corruption detection, efficient I/O | [ADR-0004](docs/adr/0004-write-ahead-log-design.md) |
| Concurrency | Single writer, lock-free reads | Simple correctness, high read throughput | [ADR-0005](docs/adr/0005-concurrency-model.md) |
| Read optimization | Per-SSTable Bloom filter | 10 bits/key, ~1% FPR, avoids unnecessary I/O | [ADR-0006](docs/adr/0006-bloom-filter-for-reads.md) |
| Caching | LRU block cache | 8MB default, reduces repeated disk reads | [ADR-0007](docs/adr/0007-block-cache-design.md) |
| Language features | Java 21 (records, sealed, virtual threads) | Less boilerplate, type safety, lightweight concurrency | [ADR-0008](docs/adr/0008-java-21-features.md) |

## Key Configuration Parameters (DBConfig)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxMemTableSize` | 4 MB | Trigger flush when MemTable exceeds this |
| `maxImmutableMemTables` | 2 | Stall writes if too many pending flushes |
| `l0CompactionTrigger` | 4 files | Trigger L0→L1 compaction |
| `l0SlowdownTrigger` | 8 files | Start slowing writes |
| `l0StopTrigger` | 12 files | Stop writes until compaction catches up |
| `l1MaxBytes` | 10 MB | Max total size for Level 1 |
| `levelSizeMultiplier` | 10 | Each level is Nx the previous |
| `maxLevels` | 7 | Maximum number of levels |
| `blockSize` | 4 KB | Target size for SSTable data blocks |
| `blockRestartInterval` | 16 | Prefix compression restart every N keys |
| `bloomFilterBitsPerKey` | 10 | Bloom filter bits per key (~1% FPR) |
| `blockCacheSize` | 8 MB | LRU cache size for data blocks |
| `walBlockSize` | 32 KB | WAL block size |
| `syncWrites` | false | fsync every write (true) vs group commit (false) |
| `verifyChecksums` | true | Verify CRC32 on reads |
