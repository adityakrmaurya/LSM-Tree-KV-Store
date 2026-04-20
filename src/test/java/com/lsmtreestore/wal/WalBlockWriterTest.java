package com.lsmtreestore.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Format-correctness tests for {@link WalBlockWriter}.
 *
 * <p>Each test writes one or more records to a real {@link FileChannel} under {@code @TempDir},
 * then reopens the file with {@link WalBlockReader} to verify byte-for-byte round-trip equality
 * plus the expected structural properties (file size, block boundaries, fragment type splits).
 */
class WalBlockWriterTest {

  // Small block size for boundary-case tests; lets us reason about exact byte positions.
  private static final int SMALL_BLOCK = 32;

  @Nested
  class Validation {

    @Test
    void constructor_nullChannel_throwsNullPointerException() {
      assertThatThrownBy(() -> new WalBlockWriter(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessageContaining("channel");
    }

    @Test
    void constructor_blockSizeAtHeaderSize_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        assertThatThrownBy(() -> new WalBlockWriter(ch, WalBlockWriter.HEADER_SIZE))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void constructor_blockSizeBelowHeaderSize_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        assertThatThrownBy(() -> new WalBlockWriter(ch, 3))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void constructor_blockSizeAllowsMaxLengthField(@TempDir Path dir) throws IOException {
      // Largest valid block is HEADER_SIZE + 0xFFFF so the Length field can still encode.
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        int max = WalBlockWriter.HEADER_SIZE + WalBlockWriter.MAX_RECORD_LENGTH;
        // Construction must not throw.
        WalBlockWriter w = new WalBlockWriter(ch, max);
        assertThat(w.blockSize()).isEqualTo(max);
      }
    }

    @Test
    void constructor_blockSizeAboveMaxLengthField_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        int over = WalBlockWriter.HEADER_SIZE + WalBlockWriter.MAX_RECORD_LENGTH + 1;
        assertThatThrownBy(() -> new WalBlockWriter(ch, over))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void write_nullPayload_throwsNullPointerException(@TempDir Path dir) throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        WalBlockWriter w = new WalBlockWriter(ch);
        assertThatThrownBy(() -> w.write(null, 0L))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("payload");
      }
    }

    @Test
    void write_emptyPayload_throwsIllegalArgumentException(@TempDir Path dir) throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        WalBlockWriter w = new WalBlockWriter(ch);
        assertThatThrownBy(() -> w.write(new byte[0], 0L))
            .isInstanceOf(IllegalArgumentException.class);
      }
    }

    @Test
    void write_negativeSeqNo_throwsIllegalArgumentException(@TempDir Path dir) throws IOException {
      try (FileChannel ch = openNew(dir.resolve("a.log"))) {
        WalBlockWriter w = new WalBlockWriter(ch);
        assertThatThrownBy(() -> w.write("x".getBytes(), -1L))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("non-negative");
      }
    }
  }

  @Nested
  class Poisoning {

    @Test
    void write_afterIoException_subsequentCallThrowsIllegalState(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // Channel that throws on every write to simulate disk failure.
      java.nio.channels.WritableByteChannel failing =
          new java.nio.channels.WritableByteChannel() {
            @Override
            public int write(java.nio.ByteBuffer src) throws IOException {
              throw new IOException("simulated disk failure");
            }

            @Override
            public boolean isOpen() {
              return true;
            }

            @Override
            public void close() {
              // no-op
            }
          };
      WalBlockWriter w = new WalBlockWriter(failing);
      assertThatThrownBy(() -> w.write("first".getBytes(), 1L))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("simulated disk failure");
      assertThat(w.isPoisoned()).isTrue();
      assertThatThrownBy(() -> w.write("second".getBytes(), 2L))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("poisoned");
      // Suppress unused-variable warning on file.
      assertThat(file).isNotNull();
    }
  }

  @Nested
  class SingleRecord {

    @Test
    void write_smallPayload_roundTripsAsFullRecord(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      byte[] payload = "hello".getBytes();
      writeOne(file, payload, 42L);
      List<WalRecord> records = readAll(file);
      assertThat(records).hasSize(1);
      assertThat(records.get(0)).isEqualTo(new WalRecord(payload, 42L, WalRecordType.FULL));
    }

    @Test
    void write_singleRecordFileSize_headerPlusVarintPlusPayload(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      byte[] payload = new byte[100];
      for (int i = 0; i < payload.length; i++) {
        payload[i] = (byte) i;
      }
      // seqNo=0 encodes as a single varint byte.
      writeOne(file, payload, 0L);
      long expected = WalBlockWriter.HEADER_SIZE + 1 + payload.length;
      assertThat(Files.size(file)).isEqualTo(expected);
    }

    @Test
    void write_largeSeqNo_encodesAsMultiByteVarint(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      byte[] payload = "x".getBytes();
      // seqNo = 2^50 takes 8 varint bytes.
      long big = 1L << 50;
      writeOne(file, payload, big);
      List<WalRecord> records = readAll(file);
      assertThat(records).hasSize(1);
      assertThat(records.get(0).seqNo()).isEqualTo(big);
      assertThat(records.get(0).payload()).isEqualTo(payload);
    }
  }

  @Nested
  class BlockBoundary {

    @Test
    void write_recordExactlyFillsBlock_emitsFullRecordNoSpillover(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // Fill SMALL_BLOCK exactly: HEADER_SIZE + varint(seqNo=0)=1 + payload = SMALL_BLOCK
      // → payload = SMALL_BLOCK - HEADER_SIZE - 1 = 24 bytes.
      byte[] payload = filled((byte) 'A', SMALL_BLOCK - WalBlockWriter.HEADER_SIZE - 1);
      writeOneWithBlockSize(file, payload, 0L, SMALL_BLOCK);

      assertThat(Files.size(file)).isEqualTo(SMALL_BLOCK);
      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(1);
      assertThat(records.get(0).payload()).isEqualTo(payload);
      assertThat(records.get(0).seqNo()).isZero();
    }

    @Test
    void write_recordStraddlesBlockBy1Byte_splitsFirstAndLast(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // One byte too big to fit in a single block, forces FIRST + LAST split.
      byte[] payload = filled((byte) 'B', SMALL_BLOCK - WalBlockWriter.HEADER_SIZE - 1 + 1);
      writeOneWithBlockSize(file, payload, 0L, SMALL_BLOCK);

      // Block 0 fully filled (SMALL_BLOCK bytes). Block 1 has HEADER + 1 data byte = 8 bytes.
      assertThat(Files.size(file)).isEqualTo(SMALL_BLOCK + WalBlockWriter.HEADER_SIZE + 1);

      // Verify the on-disk fragment-type sequence at the expected byte offsets.
      byte[] raw = Files.readAllBytes(file);
      assertThat(raw[6]).as("block 0 type byte").isEqualTo(WalRecordType.FIRST.code());
      assertThat(raw[SMALL_BLOCK + 6]).as("block 1 type byte").isEqualTo(WalRecordType.LAST.code());

      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(1);
      assertThat(records.get(0).payload()).isEqualTo(payload);
    }

    @Test
    void write_recordSpansThreeBlocks_splitsFirstMiddleLast(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      // Large enough to guarantee FIRST + MIDDLE + LAST under a 32-byte block.
      // Data needed = > 2 * (SMALL_BLOCK - HEADER_SIZE) = > 50 bytes.
      // With varint(seqNo=0) = 1 byte: payload > 49. Pick 60 → data = 61, produces FIRST(25) +
      // MIDDLE(25) + LAST(11).
      byte[] payload = filled((byte) 'C', 60);
      writeOneWithBlockSize(file, payload, 0L, SMALL_BLOCK);

      // 3 blocks: block 0 full (32), block 1 full (32), block 2 partial: HEADER + 11 = 18.
      long expected = 2L * SMALL_BLOCK + WalBlockWriter.HEADER_SIZE + 11;
      assertThat(Files.size(file)).isEqualTo(expected);

      // Verify on-disk fragment type sequence: FIRST in block 0, MIDDLE in block 1, LAST in
      // block 2. Without this, a writer bug emitting FIRST + LAST + LAST with correct lengths
      // would still round-trip via the reader's reassembly.
      byte[] raw = Files.readAllBytes(file);
      assertThat(raw[6]).as("block 0 type").isEqualTo(WalRecordType.FIRST.code());
      assertThat(raw[SMALL_BLOCK + 6]).as("block 1 type").isEqualTo(WalRecordType.MIDDLE.code());
      assertThat(raw[2 * SMALL_BLOCK + 6]).as("block 2 type").isEqualTo(WalRecordType.LAST.code());

      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(1);
      assertThat(records.get(0).payload()).isEqualTo(payload);
    }

    @Test
    void write_blockRemainingExactlyHeaderSize_padsAndStartsFresh(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // Record A: payload 17 bytes (varint(0)=1, data=18, header+data=25, blockRemaining=7).
      // Then writing record B: writer must treat blockRemaining==HEADER_SIZE as no-room and
      // pad the 7 bytes before starting B in block 1. (Pre-fix this case would emit a
      // zero-data fragment.)
      byte[] payloadA = filled((byte) 'P', 17);
      byte[] payloadB = "next".getBytes();

      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch, SMALL_BLOCK);
        w.write(payloadA, 0L);
        w.write(payloadB, 1L);
      }

      long expected = (long) SMALL_BLOCK + WalBlockWriter.HEADER_SIZE + 1 + payloadB.length;
      assertThat(Files.size(file)).isEqualTo(expected);

      // Bytes 25..31 should be zero pad (the 7-byte trailer).
      byte[] raw = Files.readAllBytes(file);
      for (int i = 25; i < SMALL_BLOCK; i++) {
        assertThat(raw[i]).as("trailer pad byte at " + i).isEqualTo((byte) 0);
      }
      // Record B starts at block 1 with type FULL (single small payload).
      assertThat(raw[SMALL_BLOCK + 6]).as("record B type").isEqualTo(WalRecordType.FULL.code());

      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(2);
      assertThat(records.get(0).payload()).isEqualTo(payloadA);
      assertThat(records.get(1).payload()).isEqualTo(payloadB);
    }
  }

  @Nested
  class TrailerPadding {

    @Test
    void write_recordLeavesLessThan7BytesInBlock_nextWritePadsAndStartsFresh(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // Record A: data = 19 bytes (varint seqNo=0 = 1 byte + payload 18 bytes).
      // header + data = 26 bytes. blockRemaining = 32 - 26 = 6 (< 7).
      // Record B: any payload. Writer must pad 6 bytes then start B at block 1.
      byte[] payloadA = filled((byte) 'P', 18);
      byte[] payloadB = filled((byte) 'Q', 4);

      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch, SMALL_BLOCK);
        w.write(payloadA, 0L);
        w.write(payloadB, 1L);
      }

      // Bytes 0..25 = record A (header + data). Bytes 26..31 = zero pad (6 bytes).
      // Bytes 32..37 = header of record B. Bytes 38..42 = data of B (varint(1)=1 + 4-byte payload).
      long expected = 32L + WalBlockWriter.HEADER_SIZE + 1 + payloadB.length;
      assertThat(Files.size(file)).isEqualTo(expected);

      // Verify the pad bytes are zeros.
      byte[] raw = Files.readAllBytes(file);
      for (int i = 26; i < 32; i++) {
        assertThat(raw[i]).as("trailer pad byte at " + i).isEqualTo((byte) 0);
      }

      // Round-trip via reader.
      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(2);
      assertThat(records.get(0)).isEqualTo(new WalRecord(payloadA, 0L, WalRecordType.FULL));
      assertThat(records.get(1)).isEqualTo(new WalRecord(payloadB, 1L, WalRecordType.FULL));
    }

    @Test
    void write_recordFillsBlockExactly_noTrailerPadBeforeNextRecord(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // Record A fills block 0 exactly (no trailer pad needed).
      byte[] payloadA = filled((byte) 'X', SMALL_BLOCK - WalBlockWriter.HEADER_SIZE - 1);
      byte[] payloadB = "next".getBytes();

      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch, SMALL_BLOCK);
        w.write(payloadA, 0L);
        w.write(payloadB, 1L);
      }

      // Block 0 exactly SMALL_BLOCK bytes, block 1 starts at byte 32 with record B.
      long expected = SMALL_BLOCK + WalBlockWriter.HEADER_SIZE + 1 + payloadB.length;
      assertThat(Files.size(file)).isEqualTo(expected);

      // No pad needed — block 0 last byte should be the last byte of record A's payload, not zero.
      byte[] raw = Files.readAllBytes(file);
      assertThat(raw[SMALL_BLOCK - 1]).isEqualTo((byte) 'X');

      List<WalRecord> records = readAllWithBlockSize(file, SMALL_BLOCK);
      assertThat(records).hasSize(2);
      assertThat(records.get(0).payload()).isEqualTo(payloadA);
      assertThat(records.get(1).payload()).isEqualTo(payloadB);
    }
  }

  @Nested
  class MultipleRecords {

    @Test
    void write_manyRecordsInOneBlock_allRoundTripInOrder(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      int count = 5;
      byte[][] payloads = new byte[count][];
      for (int i = 0; i < count; i++) {
        payloads[i] = ("record-" + i).getBytes();
      }

      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        for (int i = 0; i < count; i++) {
          w.write(payloads[i], i);
        }
      }

      List<WalRecord> records = readAll(file);
      assertThat(records).hasSize(count);
      for (int i = 0; i < count; i++) {
        assertThat(records.get(i)).isEqualTo(new WalRecord(payloads[i], i, WalRecordType.FULL));
      }
    }

    @Test
    void write_mixOfFullAndMultiFragmentRecords_roundTrips(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      byte[] small = "small".getBytes();
      byte[] big = filled((byte) 'Z', 100000); // > 3 blocks at default 32KB
      byte[] trailing = "trailing".getBytes();

      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write(small, 1L);
        w.write(big, 2L);
        w.write(trailing, 3L);
      }

      List<WalRecord> records = readAll(file);
      assertThat(records).hasSize(3);
      assertThat(records.get(0)).isEqualTo(new WalRecord(small, 1L, WalRecordType.FULL));
      assertThat(records.get(1).seqNo()).isEqualTo(2L);
      assertThat(records.get(1).payload()).isEqualTo(big);
      assertThat(records.get(2)).isEqualTo(new WalRecord(trailing, 3L, WalRecordType.FULL));
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static FileChannel openNew(Path file) throws IOException {
    return FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
  }

  private static void writeOne(Path file, byte[] payload, long seqNo) throws IOException {
    try (FileChannel ch = openNew(file)) {
      WalBlockWriter w = new WalBlockWriter(ch);
      w.write(payload, seqNo);
    }
  }

  private static void writeOneWithBlockSize(Path file, byte[] payload, long seqNo, int blockSize)
      throws IOException {
    try (FileChannel ch = openNew(file)) {
      WalBlockWriter w = new WalBlockWriter(ch, blockSize);
      w.write(payload, seqNo);
    }
  }

  private static List<WalRecord> readAll(Path file) throws IOException {
    try (WalBlockReader r = new WalBlockReader(file)) {
      return r.readAll();
    }
  }

  private static List<WalRecord> readAllWithBlockSize(Path file, int blockSize) throws IOException {
    try (WalBlockReader r = new WalBlockReader(file, blockSize)) {
      return r.readAll();
    }
  }

  private static byte[] filled(byte value, int length) {
    byte[] out = new byte[length];
    java.util.Arrays.fill(out, value);
    return out;
  }
}
