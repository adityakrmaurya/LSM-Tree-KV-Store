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
 * remain at the end of a block (not enough for a header), those bytes are zero-padded and the next
 * record starts at the beginning of the next block.
 *
 * <p>Each record's header is:
 *
 * <pre>
 *   [CRC32C masked: 4 bytes, little-endian]
 *   [Length:        2 bytes, little-endian]
 *   [Type:          1 byte: FULL=1, FIRST=2, MIDDLE=3, LAST=4]
 *   [Data:          Length bytes]
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
 */
public final class WalBlockWriter {

  /** Default block size matching LevelDB / RocksDB and ADR-0004. */
  public static final int DEFAULT_BLOCK_SIZE = 32 * 1024;

  /** Size of the record header (CRC + Length + Type) in bytes. */
  public static final int HEADER_SIZE = 7;

  /** Maximum Length value the 2-byte length field can encode (0–65535). */
  public static final int MAX_RECORD_LENGTH = 0xFFFF;

  private final WritableByteChannel channel;
  private final int blockSize;

  // Bytes already written into the current block. Resets to 0 at block boundaries.
  private int bytesInCurrentBlock;

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
   * @param blockSize the block size in bytes; must be &gt; {@link #HEADER_SIZE} and fit the 2-byte
   *     Length field, so in practice {@code HEADER_SIZE &lt; blockSize &le; MAX_RECORD_LENGTH +
   *     HEADER_SIZE}
   * @throws NullPointerException if {@code channel} is {@code null}
   * @throws IllegalArgumentException if {@code blockSize} is out of range
   */
  public WalBlockWriter(WritableByteChannel channel, int blockSize) {
    this.channel = Objects.requireNonNull(channel, "channel");
    if (blockSize <= HEADER_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be > HEADER_SIZE (" + HEADER_SIZE + "), got " + blockSize);
    }
    if (blockSize - HEADER_SIZE > MAX_RECORD_LENGTH) {
      throw new IllegalArgumentException(
          "blockSize - HEADER_SIZE must fit the 2-byte Length field (<= "
              + MAX_RECORD_LENGTH
              + "), got "
              + blockSize);
    }
    this.blockSize = blockSize;
    this.bytesInCurrentBlock = 0;
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
   * Appends one logical WAL record to the channel, splitting across blocks as needed.
   *
   * <p>The on-wire Data for this record is {@code varint(seqNo) || payload}. If the combined size
   * exceeds what fits in the remaining space of the current block, the record is split into FIRST /
   * MIDDLE* / LAST fragments. If the current block has fewer than {@link #HEADER_SIZE} bytes
   * remaining, those bytes are zero-padded before writing the next record.
   *
   * @param payload the caller's opaque payload bytes; must not be {@code null} and must be
   *     non-empty
   * @param seqNo the sequence number to persist alongside the payload (varint-encoded as a prefix
   *     of the on-wire Data); any non-negative value is valid, values are encoded unsigned-varint
   * @throws NullPointerException if {@code payload} is {@code null}
   * @throws IllegalArgumentException if {@code payload} is empty
   * @throws IOException if the underlying channel write fails
   */
  public void write(byte[] payload, long seqNo) throws IOException {
    Objects.requireNonNull(payload, "payload");
    if (payload.length == 0) {
      throw new IllegalArgumentException("payload must be non-empty");
    }

    // Compose the on-wire data: varint(seqNo) || payload. One allocation.
    int seqNoSize = Coding.varintSize(seqNo);
    byte[] data = new byte[seqNoSize + payload.length];
    Coding.encodeVarint64(data, 0, seqNo);
    System.arraycopy(payload, 0, data, seqNoSize, payload.length);

    int offset = 0;
    int remaining = data.length;
    boolean first = true;

    while (remaining > 0) {
      int blockRemaining = blockSize - bytesInCurrentBlock;
      if (blockRemaining < HEADER_SIZE) {
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

    // Length (bytes 4..5, little-endian).
    Coding.encodeFixed16(frame, 4, dataLength);
    // Type (byte 6).
    frame[6] = type.code();
    // Data (bytes 7..7+dataLength).
    System.arraycopy(data, dataOffset, frame, HEADER_SIZE, dataLength);

    // CRC32C over type + data, masked, written into bytes 0..3.
    int crc = Checksum.compute(frame, 6, 1 + dataLength);
    Coding.encodeFixed32(frame, 0, Checksum.mask(crc));

    writeFully(ByteBuffer.wrap(frame));
    bytesInCurrentBlock += totalSize;
    if (bytesInCurrentBlock == blockSize) {
      bytesInCurrentBlock = 0;
    }
  }

  /**
   * Zero-pads the remainder of the current block (when fewer than {@link #HEADER_SIZE} bytes are
   * left) and resets the block position.
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
   * Writes the entire buffer to the channel, looping until position == limit.
   *
   * <p>{@link WritableByteChannel#write(ByteBuffer)} is permitted to do a short write. Concrete
   * subclasses like {@code FileChannel} on local disk rarely short-write, but other channel
   * implementations (sockets, test doubles) may.
   */
  private void writeFully(ByteBuffer buf) throws IOException {
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
  }
}
