# ADR-0004: Write-Ahead Log Design

## Status

Accepted

## Context

The Write-Ahead Log (WAL) ensures durability: every write is appended to the WAL **before** being
applied to the MemTable. If the process crashes, the WAL is replayed on recovery to reconstruct
the MemTable state that hadn't been flushed to SSTables yet.

The WAL format must:
1. Detect corruption (partial writes, bit flips)
2. Handle records that span multiple disk blocks
3. Support efficient sequential writes (append-only)
4. Enable fast recovery (sequential read, minimal parsing)
5. Be simple enough to implement correctly (bugs here = data loss)

## Decision

Use a **LevelDB-style block-based WAL format** with CRC32 checksums.

### WAL File Structure

A WAL file is a sequence of fixed-size **blocks** (default: 32 KB each):

```
┌──────────────────────────────┐
│          Block 0 (32 KB)     │
│  [Record][Record]...[Padding]│
├──────────────────────────────┤
│          Block 1 (32 KB)     │
│  [Record][Record]...[Padding]│
├──────────────────────────────┤
│          ...                 │
└──────────────────────────────┘
```

### Record Format

Each record within a block:

```
[CRC32: 4 bytes]     // CRC32C of type + data bytes
[Length: 2 bytes]     // little-endian, length of data payload
[Type: 1 byte]       // FULL=1, FIRST=2, MIDDLE=3, LAST=4
[Data: Length bytes]  // the payload
```

**Header overhead**: 7 bytes per record.

### Record Types

Large entries that don't fit in the remaining space of the current block are split across
multiple records:

| Type | Meaning |
|------|---------|
| `FULL` (1) | The entire entry fits in a single record |
| `FIRST` (2) | First fragment of a multi-block entry |
| `MIDDLE` (3) | Interior fragment(s) of a multi-block entry |
| `LAST` (4) | Final fragment of a multi-block entry |

If the remaining space in a block is < 7 bytes (header size), it is filled with zero padding
and the next record starts at the beginning of the next block.

### Data Payload Format (WriteBatch)

The data payload for a single WAL record encodes a `WriteBatch`:

```
[Sequence number: 8 bytes]      // starting sequence number for this batch
[Count: 4 bytes]                // number of operations in the batch
[Op 0][Op 1]...[Op N]          // individual operations

Op (PUT):
  [Type: 1 byte]               // 0x01 = PUT
  [Key length: varint]
  [Key bytes]
  [Value length: varint]
  [Value bytes]

Op (DELETE):
  [Type: 1 byte]               // 0x02 = DELETE
  [Key length: varint]
  [Key bytes]
```

### WAL Lifecycle

1. A new WAL file is created when a new MutableMemTable is activated
2. Writes append records to the current WAL
3. When the MemTable is flushed to an SSTable, the corresponding WAL file is deleted
4. On crash recovery, any WAL files that exist are replayed in order

### fsync Policy

- **Default (syncWrites=false)**: Group commit — the WAL writer batches multiple writes and
  calls `fsync` once for the batch. This amortizes the expensive disk sync across many writes.
  A crash may lose the last few milliseconds of un-synced writes.
- **Durable (syncWrites=true)**: `fsync` after every write. Guarantees no data loss on crash
  at the cost of higher write latency (~1ms per write for spinning disk, ~50μs for SSD).

### Why 32 KB blocks

- Large enough to amortize seek overhead on spinning disks
- Small enough that tail padding waste is minimal (~0.1% at steady state)
- Same choice as LevelDB — proven in production
- Block boundaries provide natural corruption containment: if a block is corrupted, only records
  in that block are lost; records in other blocks can still be recovered

## Consequences

### Positive
- CRC32 checksums detect corruption from partial writes, bit flips, and filesystem bugs
- Block-based layout provides corruption isolation — a corrupt block doesn't invalidate the entire WAL
- Fixed-size blocks enable efficient sequential I/O
- Record type system handles arbitrarily large entries without memory issues
- Group commit amortizes fsync cost for high-throughput workloads
- Simple format — easy to implement correctly and debug

### Negative
- 7-byte overhead per record (~0.07% for 10KB entries, ~7% for 100-byte entries)
- Block padding wastes some space (typically < 0.5%)
- Group commit mode can lose the last few un-synced writes on crash

### Risks
- CRC32 has a 1 in 2^32 chance of missing a multi-bit corruption. Acceptable for our use case;
  if stronger guarantees are needed, can upgrade to CRC32C with hardware acceleration or xxHash.
- If fsync is not actually durable on the underlying storage (e.g., some cloud block stores with
  volatile caches), the WAL guarantee is void. This is a hardware/OS concern, not a format concern.

## References

- [LevelDB Log Format](https://github.com/google/leveldb/blob/main/doc/log_format.md)
- [RocksDB WAL Format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)
