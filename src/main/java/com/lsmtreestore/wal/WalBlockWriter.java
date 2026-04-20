package com.lsmtreestore.wal;

import com.lsmtreestore.common.Checksum;
import com.lsmtreestore.common.Coding;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/**
 * Writes WAL records to a 32 KB block-framed file per ADR-0004.
 *
 * <p>The on-disk layout is LevelDB-compatible: the file is a sequence of fixed-size blocks (default
 * 32 KB), and each block contains one or more 7-byte-header-prefixed records. Records larger than a
 * single block are split across {@link WalRecordType#FIRST}, zero or more {@link
 * WalRecordType#MIDDLE} fragments, and a {@link WalRecordType#LAST} record. If fewer than 7 bytes
 * remain at the end of a block (or exactly 7, since a header alone wastes the block), those bytes
 * are zero-padded and the next record starts at the beginning of the next block.
 *
 * <p>Each record's header is:
 *
 * <pre>
 *   [CRC32C masked: 4 bytes, little-endian] (offset 0)
 *   [Length:        2 bytes, little-endian] (offset 4)
 *   [Type:          1 byte: FULL=1, FIRST=2, MIDDLE=3, LAST=4] (offset 6)
 *   [Data:          Length bytes]                              (offset 7)
 * </pre>
 *
 * <p>The CRC covers the Type byte plus the Data bytes, then is masked via {@link
 * Checksum#mask(int)} (LevelDB-compatible) to avoid self-reference collisions during recovery
 * scans.
 *
 * <p><strong>Data convention (T5-A from eng review):</strong> each record's Data section starts
 * with a varint-encoded {@code seqNo} followed by the caller's opaque payload bytes. The writer
 * prepends the varint automatically; the reader strips it. This keeps the on-disk record header
 * from ADR-0004 unchanged while still persisting the coordinator's per-record sequence number.
 *
 * <p><strong>Thread safety:</strong> this class is <strong>not</strong> thread-safe. A single WAL
 * file has exactly one {@code WalBlockWriter} instance, and the coordinator (forthcoming) is
 * responsible for serializing access to it.
 *
 * <p><strong>Failure semantics:</strong> any {@link IOException} thrown from {@link #write(byte[],
 * long)} marks the writer as poisoned. After poisoning, every subsequent {@code write()} call fails
 * fast with {@link IllegalStateException} — the channel cursor and the writer's block-position
 * counter may have diverged on a partial-write IOException, so continuing would silently corrupt
 * subsequent block boundaries. The owner (typically the WAL coordinator) is responsible for
 * discarding a poisoned writer and any in-progress batch.
 */
public final class WalBlockWriter {

  /** Default block size matching LevelDB / RocksDB and ADR-0004. */
  public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;

  /** Size of the record header (CRC + Length + Type) in bytes. */
  public static final int HEADER_SIZE = 7;

  /** Maximum Length value the 2-byte length field can encode (0–65535). */
  public static final int MAX_RECORD_LENGTH = 0xFFFF;

  /** Maximum legal block size: header + max length field. */
  public static final int MAX_BLOCK_SIZE = HEADER_SIZE + MAX_RECORD_LENGTH;

  // Header field byte offsets within a record frame. Shared with WalBlockReader.
  static final int CRC_OFFSET = 0;
  static final int LENGTH_OFFSET = 4;
  static final int TYPE_OFFSET = 6;
  static final int DATA_OFFSET = HEADER_SIZE;

  private final WritableByteChannel channel;
  private final int blockSize;

  // Bytes already written into the current block. Resets to 0 at block boundaries.
  private int bytesInCurrentBlock;

  // Set to true after any IOException from writeFully; subsequent writes fail fast.
  // volatile so that a coordinator handing the writer across threads via any synchronizer
  // still sees the correct poisoned state — even if the transfer primitive itself doesn't
  // establish a happens-before edge for this particular field.
  private volatile boolean poisoned;

  /**
   * Constructs a writer over the given channel using the default 32 KB block size.
   *
   * @param channel the channel to write framed bytes into; must be open for writing
   * @throws NullPointerException if {@code channel} is {@code null}
   */
  public WalBlockWriter(WritableByteChannel channel) {
    this(channel, DEFAULT_BLOCK_SIZE);
  }

  /**
   * Constructs a writer over the given channel with a custom block size.
   *
   * @param channel the channel to write framed bytes into; must be open for writing
   * @param blockSize the block size in bytes; must satisfy {@code HEADER_SIZE < blockSize <=
   *     MAX_BLOCK_SIZE} (i.e. the data portion of a single block must fit the 2-byte Length field)
   * @throws NullPointerException if {@code channel} is {@code null}
   * @throws IllegalArgumentException if {@code blockSize} is out of range
   */
  public WalBlockWriter(WritableByteChannel channel, int blockSize) {
    this.channel = Objects.requireNonNull(channel, "channel");
    if (blockSize <= HEADER_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be > HEADER_SIZE (" + HEADER_SIZE + "), got " + blockSize);
    }
    if (blockSize > MAX_BLOCK_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be <= MAX_BLOCK_SIZE (" + MAX_BLOCK_SIZE + "), got " + blockSize);
    }
    this.blockSize = blockSize;
    this.bytesInCurrentBlock = 0;
    this.poisoned = false;
  }

  /**
   * Returns the configured block size in bytes.
   *
   * @return the block size (32 KB by default)
   */
  public int blockSize() {
    return blockSize;
  }

  /**
   * Returns whether this writer has been marked unusable due to a prior {@link IOException}.
   *
   * @return true if any prior {@code write()} threw {@link IOException}
   */
  public boolean isPoisoned() {
    return poisoned;
  }

  /**
   * Appends one logical WAL record to the channel, splitting across blocks as needed.
   *
   * <p>The on-wire Data for this record is {@code varint(seqNo) || payload}. If the combined size
   * exceeds what fits in the remaining space of the current block, the record is split into FIRST /
   * MIDDLE* / LAST fragments. If the current block has 7 or fewer bytes remaining (not enough for a
   * header + at least one data byte), those bytes are zero-padded before writing the next record.
   *
   * @param payload the caller's opaque payload bytes; must not be {@code null} and must be
   *     non-empty
   * @param seqNo the sequence number to persist alongside the payload (varint-encoded as a prefix
   *     of the on-wire Data); must be non-negative
   * @throws NullPointerException if {@code payload} is {@code null}
   * @throws IllegalArgumentException if {@code payload} is empty, {@code seqNo} is negative, or the
   *     composed data length would overflow {@code int}
   * @throws IllegalStateException if this writer was previously poisoned by an {@link IOException}
   * @throws IOException if the underlying channel write fails (also poisons the writer)
   */
  public void write(byte[] payload, long seqNo) throws IOException {
    if (poisoned) {
      throw new IllegalStateException("writer is poisoned (prior IOException)");
    }
    Objects.requireNonNull(payload, "payload");
    if (payload.length == 0) {
      throw new IllegalArgumentException("payload must be non-empty");
    }
    if (seqNo < 0) {
      throw new IllegalArgumentException("seqNo must be non-negative, got " + seqNo);
    }

    // Compose the on-wire data: varint(seqNo) || payload. Use long math to detect overflow before
    // the new byte[] allocation would silently flip negative.
    int seqNoSize = Coding.varintSize(seqNo);
    long composedLength = (long) seqNoSize + (long) payload.length;
    if (composedLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "composed record data length overflows int: seqNoSize="
              + seqNoSize
              + ", payloadLength="
              + payload.length);
    }
    byte[] data = new byte[(int) composedLength];
    Coding.encodeVarint64(data, 0, seqNo);
    System.arraycopy(payload, 0, data, seqNoSize, payload.length);

    int offset = 0;
    int remaining = data.length;
    boolean first = true;

    while (remaining > 0) {
      int blockRemaining = blockSize - bytesInCurrentBlock;
      // Strictly less than HEADER_SIZE + 1 bytes left = no room for a header AND a data byte.
      // Equality (blockRemaining == HEADER_SIZE) would emit a zero-data fragment that wastes
      // space and creates a fragment the reader has to handle for no value — pad instead.
      if (blockRemaining <= HEADER_SIZE) {
        padBlockTrailer(blockRemaining);
        blockRemaining = blockSize;
      }
      int maxPayloadThisRecord = blockRemaining - HEADER_SIZE;
      int dataChunkSize = Math.min(remaining, maxPayloadThisRecord);
      boolean last = (dataChunkSize == remaining);

      WalRecordType type;
      if (first && last) {
        type = WalRecordType.FULL;
      } else if (first) {
        type = WalRecordType.FIRST;
      } else if (last) {
        type = WalRecordType.LAST;
      } else {
        type = WalRecordType.MIDDLE;
      }

      emitRecord(type, data, offset, dataChunkSize);

      offset += dataChunkSize;
      remaining -= dataChunkSize;
      first = false;
    }
  }

  /**
   * Writes one on-disk record fragment: header + data slice.
   *
   * @param type the record type for this fragment
   * @param data the source array containing the data bytes
   * @param dataOffset starting index in {@code data}
   * @param dataLength number of bytes of data to write (must fit the 2-byte Length field)
   */
  private void emitRecord(WalRecordType type, byte[] data, int dataOffset, int dataLength)
      throws IOException {
    int totalSize = HEADER_SIZE + dataLength;
    byte[] frame = new byte[totalSize];

    Coding.encodeFixed16(frame, LENGTH_OFFSET, dataLength);
    frame[TYPE_OFFSET] = type.code();
    System.arraycopy(data, dataOffset, frame, DATA_OFFSET, dataLength);

    // CRC32C over [type | data], masked, written into the CRC slot.
    int crc = Checksum.compute(frame, TYPE_OFFSET, 1 + dataLength);
    Coding.encodeFixed32(frame, CRC_OFFSET, Checksum.mask(crc));

    writeFully(ByteBuffer.wrap(frame));
    bytesInCurrentBlock += totalSize;
    if (bytesInCurrentBlock == blockSize) {
      bytesInCurrentBlock = 0;
    }
  }

  /**
   * Zero-pads the remainder of the current block (when the room left can't fit a header + at least
   * one data byte) and resets the block position.
   *
   * @param padBytes number of zero bytes to write to fill out the block
   */
  private void padBlockTrailer(int padBytes) throws IOException {
    if (padBytes == 0) {
      return;
    }
    ByteBuffer pad = ByteBuffer.allocate(padBytes);
    // Java zeros new buffers by default; no explicit fill needed.
    writeFully(pad);
    bytesInCurrentBlock = 0;
  }

  /**
   * Writes the entire buffer to the channel, looping until position == limit. On any {@link
   * IOException}, marks the writer as poisoned and rethrows; the channel cursor and the writer's
   * block-position counter may have diverged after a partial write, so subsequent writes would
   * corrupt block boundaries.
   */
  private void writeFully(ByteBuffer buf) throws IOException {
    try {
      while (buf.hasRemaining()) {
        int written = channel.write(buf);
        if (written == 0) {
          // FileChannel over regular files won't return 0 with a non-empty buffer, but
          // other WritableByteChannel implementations (non-blocking sockets, flow-controlled
          // pipes, or test doubles) can. Treat as I/O failure rather than spinning.
          throw new IOException(
              "Channel returned 0 bytes with "
                  + buf.remaining()
                  + " remaining; aborting to"
                  + " avoid a livelock");
        }
      }
    } catch (IOException ioe) {
      poisoned = true;
      throw ioe;
    }
  }
}
