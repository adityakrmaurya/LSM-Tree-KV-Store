# ADR-0003: SSTable File Format

## Status

Accepted

## Context

SSTables (Sorted String Tables) are the persistent, immutable, on-disk files that store key-value
pairs in sorted order. The file format determines:

- **Read efficiency** — how quickly we can locate a key within a file
- **Write efficiency** — how quickly we can build the file during flush/compaction
- **Space efficiency** — how much overhead the format adds
- **Cache friendliness** — how well the format works with OS page cache and our block cache

We need a format that supports:
1. Point lookups via binary search (not full scan)
2. Sequential iteration for range scans and compaction
3. Bloom filter for fast negative lookups
4. Prefix compression to reduce key storage overhead
5. Checksums for data integrity

## Decision

Use a **LevelDB-inspired block-based SSTable format**. This is a proven design used by LevelDB
and (with extensions) by RocksDB.

### File Layout

```
┌─────────────────────────────────┐  offset 0
│         Data Block 0            │
│         Data Block 1            │
│         ...                     │
│         Data Block N            │
├─────────────────────────────────┤
│         Filter Block            │  Bloom filter for all keys
├─────────────────────────────────┤
│       Meta-Index Block          │  Maps "filter.bloom" → BlockHandle
├─────────────────────────────────┤
│         Index Block             │  Maps separator_key_i → BlockHandle(Data Block i)
├─────────────────────────────────┤
│          Footer (48 bytes)      │  meta-index handle + index handle + magic
└─────────────────────────────────┘  EOF
```

### Data Block Format

Each data block is a self-contained sorted sequence of key-value entries with prefix compression.

```
┌──────────────────────────────────────────────────────┐
│ Entry 0: [shared_len][non_shared_len][val_len][key][value] │
│ Entry 1: [shared_len][non_shared_len][val_len][key][value] │
│ ...                                                        │
│ Entry K                                                    │
├──────────────────────────────────────────────────────┤
│ Restart[0] (4 bytes, little-endian)                  │
│ Restart[1] (4 bytes, little-endian)                  │
│ ...                                                  │
│ Restart[R] (4 bytes, little-endian)                  │
│ NumRestarts (4 bytes, little-endian)                 │
└──────────────────────────────────────────────────────┘
```

- **Prefix compression**: Each entry stores only the bytes that differ from the previous key.
  `shared_len` = number of bytes shared with previous key, `non_shared_len` = unique bytes.
- **Restart points**: Every `blockRestartInterval` entries (default: 16), `shared_len` is forced
  to 0 (full key stored). Restart point offsets are stored at the end of the block. This enables
  binary search within the block via restart points.
- **Lengths are varint-encoded** to minimize overhead for small keys/values.

### Entry Format (within a data block)

```
[shared_key_length: varint]       // bytes shared with previous key
[non_shared_key_length: varint]   // bytes unique to this key
[value_length: varint]            // length of value
[non_shared_key_bytes: byte[]]    // the unique key suffix
[value_bytes: byte[]]             // the value
```

### Internal Key Format

User keys are wrapped in an internal key for versioning and delete support:

```
[user_key_bytes: byte[]]          // the user-supplied key
[sequence_number | type: 8 bytes] // packed as (sequenceNumber << 8) | type
```

- `type`: `0x01` = PUT, `0x00` = DELETE
- Keys sort by: user_key ascending → sequence_number descending → type descending
- This ordering ensures the newest version of a key is encountered first during iteration

### Index Block

Same format as a data block, but entries map the **last key in each data block** to its
`BlockHandle(offset, size)`. Binary search on the index block locates the right data block
for a given key in O(log B) where B = number of data blocks.

### Filter Block (Bloom Filter)

Contains the serialized Bloom filter bits for all keys in the SSTable. Stored as:

```
[filter_bits: byte[]]            // the Bloom filter bit array
[num_hash_functions: 1 byte]     // number of hash functions (k)
```

### Meta-Index Block

Maps filter names to their BlockHandle. Currently only one entry:
`"filter.bloom"` → `BlockHandle(offset, size)` of the Filter Block.

### Footer (Fixed 48 bytes)

```
[meta_index_handle: BlockHandle]  // padded to 20 bytes
[index_handle: BlockHandle]       // padded to 20 bytes
[magic_number: 8 bytes]           // 0xdb4775248b80fb57 (same as LevelDB)
```

Each `BlockHandle` is varint-encoded `(offset, size)`, padded with zeros to exactly 20 bytes.
The fixed footer size allows reading it from a known offset (fileSize - 48).

### Default Parameters

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| Block size | 4 KB | Matches typical OS page size / SSD read unit. Good cache granularity. |
| Restart interval | 16 | ~6% overhead for restart points. Balances prefix compression vs seek speed. |
| Magic number | `0xdb4775248b80fb57` | LevelDB compatibility. Detects file corruption / wrong file type. |

## Consequences

### Positive
- Block-aligned reads are cache-friendly (both OS page cache and our LRU block cache)
- Prefix compression reduces key storage by 30-60% for keys with common prefixes
- Two-level lookup (index → data block) keeps index small enough to hold in memory
- Bloom filter avoids reading data blocks for absent keys
- Footer at fixed offset from EOF enables fast file opening
- Proven format with extensive production history

### Negative
- Prefix compression adds complexity to block building and reading
- Varint encoding adds CPU cost (though minimal — a few nanoseconds per decode)
- Fixed 48-byte footer wastes a few bytes for small BlockHandles
- No built-in block-level compression (can be added later as a block wrapper)

### Risks
- Very large values (>blockSize) produce blocks larger than the target size. Mitigated by flushing
  the current block before adding an entry that would exceed 2× the target block size.
- Corruption in the index block makes the entire SSTable unreadable. Mitigated by CRC checksums
  per block (to be added as a block trailer in a future iteration).

## References

- [LevelDB Table Format](https://github.com/google/leveldb/blob/main/doc/table_format.md)
- [RocksDB BlockBasedTable Format](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format)
- [LevelDB impl.md](https://github.com/google/leveldb/blob/main/doc/impl.md)
