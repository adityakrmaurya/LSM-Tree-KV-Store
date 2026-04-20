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
 * truncated data, or malformed fragment sequence all throw {@link CorruptionException}.
 */
public final class WalBlockReader implements AutoCloseable {

  private final Path file;
  private final FileChannel channel;
  private final int blockSize;
  private final long fileSize;

  // Bytes already consumed from the current block. Resets to 0 at block boundaries.
  private long position;

  /**
   * Opens a WAL file for sequential reading using the default 32 KB block size.
   *
   * @param file path to the WAL file to read
   * @throws IOException if the file cannot be opened
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public WalBlockReader(Path file) throws IOException {
    this(file, WalBlockWriter.DEFAULT_BLOCK_SIZE);
  }

  /**
   * Opens a WAL file for sequential reading with a custom block size.
   *
   * <p>The block size must match the size that was used when the file was written, otherwise
   * trailer padding will be misinterpreted as record data and CRC checks will fail.
   *
   * @param file path to the WAL file to read
   * @param blockSize block size in bytes; must match the writer's block size
   * @throws IOException if the file cannot be opened
   * @throws NullPointerException if {@code file} is {@code null}
   * @throws IllegalArgumentException if {@code blockSize} is &le; {@link
   *     WalBlockWriter#HEADER_SIZE}
   */
  public WalBlockReader(Path file, int blockSize) throws IOException {
    this.file = Objects.requireNonNull(file, "file");
    if (blockSize <= WalBlockWriter.HEADER_SIZE) {
      throw new IllegalArgumentException(
          "blockSize must be > HEADER_SIZE (" + WalBlockWriter.HEADER_SIZE + "), got " + blockSize);
    }
    this.channel = FileChannel.open(file, StandardOpenOption.READ);
    this.blockSize = blockSize;
    this.fileSize = Files.size(file);
    this.position = 0;
  }

  /**
   * Reads the next logical record from the file.
   *
   * @return the next {@link WalRecord}, or {@link Optional#empty()} if end-of-file was reached
   *     cleanly at a record boundary
   * @throws CorruptionException if the file is truncated mid-record, a CRC fails, a type code is
   *     unknown, or fragments appear in an illegal sequence (e.g., MIDDLE without a preceding
   *     FIRST)
   * @throws IOException if the underlying channel read fails
   */
  public Optional<WalRecord> readNext() throws IOException {
    Fragment first = readNextFragment();
    if (first == null) {
      return Optional.empty();
    }
    byte[] recordData;
    WalRecordType logicalType = WalRecordType.FULL;
    if (first.type == WalRecordType.FULL) {
      recordData = first.data;
    } else if (first.type == WalRecordType.FIRST) {
      ByteArrayOutputStream acc = new ByteArrayOutputStream();
      acc.write(first.data, 0, first.data.length);
      while (true) {
        Fragment next = readNextFragment();
        if (next == null) {
          throw new CorruptionException(
              "Truncated multi-fragment record: missing LAST after FIRST/MIDDLE", file, position);
        }
        switch (next.type) {
          case MIDDLE -> acc.write(next.data, 0, next.data.length);
          case LAST -> {
            acc.write(next.data, 0, next.data.length);
            recordData = acc.toByteArray();
            return Optional.of(decodeLogical(recordData, logicalType));
          }
          default ->
              throw new CorruptionException(
                  "Unexpected fragment type inside multi-fragment record: " + next.type,
                  file,
                  position);
        }
      }
    } else {
      throw new CorruptionException(
          "Record sequence started with " + first.type + "; expected FULL or FIRST",
          file,
          position);
    }
    return Optional.of(decodeLogical(recordData, logicalType));
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
    // Skip trailer padding if the remaining bytes in this block can't fit a header.
    long blockOffset = position % blockSize;
    long blockRemaining = blockSize - blockOffset;
    if (blockRemaining < WalBlockWriter.HEADER_SIZE) {
      position += blockRemaining;
      if (position >= fileSize) {
        return null;
      }
    }

    byte[] header = readExactly(WalBlockWriter.HEADER_SIZE);
    int maskedCrc = Coding.decodeFixed32(header, 0);
    int length = Coding.decodeFixed16(header, 4);
    byte typeCode = header[6];
    WalRecordType type = WalRecordType.fromByte(typeCode);

    if (length > blockSize - WalBlockWriter.HEADER_SIZE) {
      throw new CorruptionException(
          "Record length " + length + " exceeds max payload per block", file, position);
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
   * @param type the logical type to report (always {@link WalRecordType#FULL} in practice)
   */
  private WalRecord decodeLogical(byte[] data, WalRecordType type) {
    int[] bytesRead = new int[1];
    long seqNo;
    try {
      seqNo = Coding.decodeVarint64(data, 0, bytesRead);
    } catch (IllegalArgumentException e) {
      throw new CorruptionException("Malformed seqNo varint at start of record data", e);
    }
    int prefixLen = bytesRead[0];
    byte[] payload = new byte[data.length - prefixLen];
    System.arraycopy(data, prefixLen, payload, 0, payload.length);
    return new WalRecord(payload, seqNo, type);
  }

  /**
   * Reads exactly {@code n} bytes from the channel starting at the current position, advancing the
   * position. Throws if EOF arrives before {@code n} bytes are read.
   */
  private byte[] readExactly(int n) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(n);
    while (buf.hasRemaining()) {
      int read = channel.read(buf, position + (n - buf.remaining()));
      if (read < 0) {
        throw new CorruptionException(
            "Truncated: expected " + n + " bytes, got " + buf.position(), file, position);
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
