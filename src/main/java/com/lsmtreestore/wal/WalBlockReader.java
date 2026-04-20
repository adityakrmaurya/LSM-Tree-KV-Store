package com.lsmtreestore.wal;

import com.lsmtreestore.common.Checksum;
import com.lsmtreestore.common.Coding;
import com.lsmtreestore.common.CorruptionException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal raw reader for WAL files in the ADR-0004 block format.
 *
 * <p>Iterates records in file order, reassembling FIRST / MIDDLE* / LAST fragments into single
 * logical {@link WalRecord}s, stripping the varint-prefix {@code seqNo} (T5-A convention) to
 * recover the caller's original payload, and validating the CRC32C on every fragment.
 *
 * <p>This is a <strong>test utility</strong> for PR #1. It is sufficient to round-trip records
 * written by {@link WalBlockWriter} and to decode them during the marquee concurrency test. It is
 * <strong>not</strong> the recovery-path reader — that reader (which must tolerate partial last
 * records and bad CRCs gracefully) lands with the MemTable module in a later PR.
 *
 * <p>Exceptions raised here are intentionally fail-fast: a CRC mismatch, unknown type byte,
 * truncated data, or malformed fragment sequence all throw {@link CorruptionException}. CRUCIALLY:
 * after any such throw, the reader's internal position may be past the corrupt record, so callers
 * should treat a thrown {@link CorruptionException} as terminal — discard the reader rather than
 * calling {@code readNext()} again.
 *
 * <p><strong>Trust boundary:</strong> this reader treats the file as untrusted input. It validates
 * every record's CRC and refuses fragments whose declared length would cross block boundaries (a
 * malicious file otherwise could desynchronize all subsequent records). It also caps the
 * reassembled size of a single multi-fragment logical record to {@link #DEFAULT_MAX_RECORD_BYTES}
 * (configurable via the constructor) to prevent OOM from a hostile FIRST + millions of MIDDLEs.
 *
 * <p><strong>Note:</strong> {@link CorruptionException} is a runtime exception (extends {@code
 * RuntimeException} via {@code StoreException}). It is documented via {@code @throws} but not
 * declared in method signatures.
 */
public final class WalBlockReader implements AutoCloseable {

  /** Default cap on the size of a reassembled multi-fragment logical record (64 MB). */
  public static final int DEFAULT_MAX_RECORD_BYTES = 64 * 1024 * 1024;

  private final Path file;
  private final FileChannel channel;
  private final int blockSize;
  private final long fileSize;
  private final int maxRecordBytes;

  // Bytes already consumed from the current block. Resets to 0 at block boundaries.
  private long position;

  /**
   * Opens a WAL file for sequential reading using the default 32 KB block size and the default
   * max-record cap.
   *
   * @param file path to the WAL file to read
   * @throws IOException if the file cannot be opened
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public WalBlockReader(Path file) throws IOException {
    this(file, WalBlockWriter.DEFAULT_BLOCK_SIZE, DEFAULT_MAX_RECORD_BYTES);
  }

  /**
   * Opens a WAL file for sequential reading with a custom block size and the default max-record
   * cap.
   *
   * @param file path to the WAL file to read
   * @param blockSize block size in bytes; must match the writer's block size and satisfy {@code
   *     HEADER_SIZE < blockSize <= MAX_BLOCK_SIZE}
   * @throws IOException if the file cannot be opened
   * @throws NullPointerException if {@code file} is {@code null}
   * @throws IllegalArgumentException if {@code blockSize} is out of range
   */
  public WalBlockReader(Path file, int blockSize) throws IOException {
    this(file, blockSize, DEFAULT_MAX_RECORD_BYTES);
  }

  /**
   * Opens a WAL file for sequential reading with a custom block size and a custom cap on the
   * largest reassembled logical record.
   *
   * <p>The block size must match the size that was used when the file was written, otherwise
   * trailer padding will be misinterpreted as record data and CRC checks will fail. There is no
   * file-level magic number to detect a mismatch, so the caller is responsible for providing the
   * correct block size.
   *
   * @param file path to the WAL file to read
   * @param blockSize block size in bytes; must match the writer's block size
   * @param maxRecordBytes maximum size in bytes that a single reassembled logical record is allowed
   *     to reach; must be positive. A FIRST/MIDDLE...LAST sequence whose accumulated data would
   *     exceed this cap is rejected as corruption (OOM defense against hostile files).
   * @throws IOException if the file cannot be opened
   * @throws NullPointerException if {@code file} is {@code null}
   * @throws IllegalArgumentException if {@code blockSize} is out of range or {@code maxRecordBytes}
   *     is not positive
   */
  public WalBlockReader(Path file, int blockSize, int maxRecordBytes) throws IOException {
    this.file = Objects.requireNonNull(file, "file");
    if (blockSize <= WalBlockWriter.HEADER_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be > HEADER_SIZE (" + WalBlockWriter.HEADER_SIZE + "), got " + blockSize);
    }
    if (blockSize > WalBlockWriter.MAX_BLOCK_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be <= MAX_BLOCK_SIZE ("
              + WalBlockWriter.MAX_BLOCK_SIZE
              + "), got "
              + blockSize);
    }
    if (maxRecordBytes <= 0) {
      throw new IllegalArgumentException("maxRecordBytes must be positive, got " + maxRecordBytes);
    }
    this.channel = FileChannel.open(file, StandardOpenOption.READ);
    this.blockSize = blockSize;
    this.fileSize = Files.size(file);
    this.maxRecordBytes = maxRecordBytes;
    this.position = 0;
  }

  /**
   * Reads the next logical record from the file.
   *
   * @return the next {@link WalRecord}, or {@link Optional#empty()} if end-of-file was reached
   *     cleanly at a record boundary
   * @throws CorruptionException if the file is truncated mid-record, a CRC fails, a type code is
   *     unknown, fragments appear in an illegal sequence (e.g., MIDDLE without a preceding FIRST),
   *     a length field would cross a block boundary, or the reassembled record exceeds the
   *     max-record cap
   * @throws IOException if the underlying channel read fails
   */
  public Optional<WalRecord> readNext() throws IOException {
    Fragment first = readNextFragment();
    if (first == null) {
      return Optional.empty();
    }
    if (first.type == WalRecordType.FULL) {
      return Optional.of(
          decodeLogical(first.data, position - first.data.length - WalBlockWriter.HEADER_SIZE));
    }
    if (first.type != WalRecordType.FIRST) {
      throw new CorruptionException(
          "Record sequence started with " + first.type + "; expected FULL or FIRST",
          file,
          position);
    }
    long recordStartOffset = position - first.data.length - WalBlockWriter.HEADER_SIZE;
    ByteArrayOutputStream acc = new ByteArrayOutputStream();
    acc.write(first.data, 0, first.data.length);
    while (true) {
      Fragment next = readNextFragment();
      if (next == null) {
        throw new CorruptionException(
            "Truncated multi-fragment record: missing LAST after FIRST/MIDDLE", file, position);
      }
      if (acc.size() + next.data.length > maxRecordBytes) {
        throw new CorruptionException(
            "Reassembled record would exceed maxRecordBytes (" + maxRecordBytes + ")",
            file,
            recordStartOffset);
      }
      switch (next.type) {
        case MIDDLE -> acc.write(next.data, 0, next.data.length);
        case LAST -> {
          acc.write(next.data, 0, next.data.length);
          return Optional.of(decodeLogical(acc.toByteArray(), recordStartOffset));
        }
        default ->
            throw new CorruptionException(
                "Unexpected fragment type inside multi-fragment record: " + next.type,
                file,
                position);
      }
    }
  }

  /**
   * Reads the remainder of the file into a list of logical records.
   *
   * @return all records from the current position to end-of-file, in file order
   * @throws CorruptionException as documented on {@link #readNext()}
   * @throws IOException if the underlying channel read fails
   */
  public List<WalRecord> readAll() throws IOException {
    List<WalRecord> out = new ArrayList<>();
    Optional<WalRecord> rec;
    while ((rec = readNext()).isPresent()) {
      out.add(rec.get());
    }
    return out;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  /**
   * Reads one on-disk fragment (header + data bytes), advancing the internal position. Returns
   * {@code null} at a clean EOF (between records, not mid-header).
   *
   * @return a {@link Fragment} with type and data bytes, or {@code null} at clean EOF
   */
  private Fragment readNextFragment() throws IOException {
    if (position >= fileSize) {
      return null;
    }
    // Skip trailer padding if the remaining bytes in this block can't fit a header AND at least
    // one data byte. Mirror the writer, which pads when blockRemaining <= HEADER_SIZE.
    long blockOffset = position % blockSize;
    long blockRemaining = blockSize - blockOffset;
    if (blockRemaining <= WalBlockWriter.HEADER_SIZE) {
      position += blockRemaining;
      if (position >= fileSize) {
        return null;
      }
      blockOffset = 0;
      blockRemaining = blockSize;
    }

    byte[] header = readExactly(WalBlockWriter.HEADER_SIZE);
    int maskedCrc = Coding.decodeFixed32(header, WalBlockWriter.CRC_OFFSET);
    int length = Coding.decodeFixed16(header, WalBlockWriter.LENGTH_OFFSET);
    byte typeCode = header[WalBlockWriter.TYPE_OFFSET];
    WalRecordType type = WalRecordType.fromByte(typeCode);

    // Cross-block validation: the writer never emits a fragment whose data crosses a block
    // boundary. A malicious or corrupt file that claims a length larger than what fits in the
    // current block would otherwise desynchronize all subsequent records.
    int maxDataInBlock = (int) (blockRemaining - WalBlockWriter.HEADER_SIZE);
    if (length > maxDataInBlock) {
      throw new CorruptionException(
          "Record length " + length + " exceeds remaining bytes in block (" + maxDataInBlock + ")",
          file,
          position - WalBlockWriter.HEADER_SIZE);
    }
    byte[] data = readExactly(length);

    // CRC is computed over [type byte | data bytes] and stored masked.
    int computed = Checksum.compute(concat(typeCode, data), 0, 1 + length);
    int expected = Checksum.unmask(maskedCrc);
    if (computed != expected) {
      throw new CorruptionException(
          "CRC mismatch: expected=0x"
              + Integer.toHexString(expected)
              + " computed=0x"
              + Integer.toHexString(computed),
          file,
          position - WalBlockWriter.HEADER_SIZE - length);
    }

    return new Fragment(type, data);
  }

  /**
   * Decodes a reassembled record: strips the varint seqNo prefix and returns the logical {@link
   * WalRecord} with the caller's original payload.
   *
   * @param data the reassembled on-wire data ({@code varint(seqNo) || payload})
   * @param recordStartOffset byte offset in the file where this logical record began (used in
   *     {@link CorruptionException} forensics)
   */
  private WalRecord decodeLogical(byte[] data, long recordStartOffset) {
    int[] bytesRead = new int[1];
    long seqNo;
    try {
      seqNo = Coding.decodeVarint64(data, 0, bytesRead);
    } catch (IllegalArgumentException e) {
      throw new CorruptionException(
          "Malformed seqNo varint at start of record data", file, recordStartOffset, e);
    }
    int prefixLen = bytesRead[0];
    int payloadLen = data.length - prefixLen;
    if (payloadLen <= 0) {
      // Symmetric with the writer (which refuses empty payload). A file with a record whose data
      // is only a varint is either corruption or hand-crafted hostile input.
      throw new CorruptionException(
          "Record data contains no payload after stripping seqNo varint", file, recordStartOffset);
    }
    byte[] payload = new byte[payloadLen];
    System.arraycopy(data, prefixLen, payload, 0, payloadLen);
    return new WalRecord(payload, seqNo, WalRecordType.FULL);
  }

  /**
   * Reads exactly {@code n} bytes from the channel starting at the current position, advancing the
   * position. Throws if EOF arrives before {@code n} bytes are read or if the channel returns 0
   * bytes (which would otherwise loop forever).
   */
  private byte[] readExactly(int n) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(n);
    while (buf.hasRemaining()) {
      int read = channel.read(buf, position + (n - buf.remaining()));
      if (read < 0) {
        throw new CorruptionException(
            "Truncated: expected " + n + " bytes, got " + buf.position(), file, position);
      }
      if (read == 0) {
        // FileChannel over a regular file shouldn't return 0 with a non-empty buffer, but other
        // channel implementations might. Treat as I/O failure rather than spinning.
        throw new IOException(
            "Channel returned 0 bytes with "
                + buf.remaining()
                + " requested at offset "
                + (position + (n - buf.remaining())));
      }
    }
    position += n;
    return buf.array();
  }

  /** Allocates a new array of {@code [type] || data} bytes for CRC computation. */
  private static byte[] concat(byte typeByte, byte[] data) {
    byte[] out = new byte[1 + data.length];
    out[0] = typeByte;
    System.arraycopy(data, 0, out, 1, data.length);
    return out;
  }

  /** Internal carrier for an on-disk fragment as read from the file. */
  private record Fragment(WalRecordType type, byte[] data) {}
}
