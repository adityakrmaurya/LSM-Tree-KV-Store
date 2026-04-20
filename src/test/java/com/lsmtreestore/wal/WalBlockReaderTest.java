package com.lsmtreestore.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lsmtreestore.common.Checksum;
import com.lsmtreestore.common.Coding;
import com.lsmtreestore.common.CorruptionException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Error-path and edge-case tests for {@link WalBlockReader}. */
class WalBlockReaderTest {

  @Nested
  class EmptyAndTrivial {

    @Test
    void readNext_emptyFile_returnsEmpty(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("empty.log");
      Files.createFile(file);
      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThat(r.readNext()).isEmpty();
      }
    }

    @Test
    void readAll_emptyFile_returnsEmptyList(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("empty.log");
      Files.createFile(file);
      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThat(r.readAll()).isEmpty();
      }
    }

    @Test
    void constructor_blockSizeBelowHeaderSize_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      Files.createFile(file);
      assertThatThrownBy(() -> new WalBlockReader(file, 3))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_blockSizeAboveMaxBlockSize_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      Files.createFile(file);
      assertThatThrownBy(() -> new WalBlockReader(file, WalBlockWriter.MAX_BLOCK_SIZE + 1))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructor_maxRecordBytesZero_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      Files.createFile(file);
      assertThatThrownBy(() -> new WalBlockReader(file, WalBlockWriter.DEFAULT_BLOCK_SIZE, 0))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("maxRecordBytes");
    }

    @Test
    void constructor_maxRecordBytesNegative_throwsIllegalArgumentException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      Files.createFile(file);
      assertThatThrownBy(() -> new WalBlockReader(file, WalBlockWriter.DEFAULT_BLOCK_SIZE, -1))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class Corruption {

    @Test
    void readNext_flippedDataByte_throwsCrcMismatch(@TempDir Path dir) throws IOException {
      Path file = dir.resolve("a.log");
      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write("hello".getBytes(), 1L);
      }
      // Flip a data byte past the header (position 8 — after the 7-byte header + 1-byte varint).
      byte[] raw = Files.readAllBytes(file);
      raw[8] ^= 0x01;
      Files.write(file, raw);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("CRC mismatch");
      }
    }

    @Test
    void readNext_unknownTypeCode_throwsCorruptionException(@TempDir Path dir) throws IOException {
      // Hand-craft a header with an invalid type byte (and a matching CRC for just the type+data).
      byte[] data = "x".getBytes();
      int length = data.length;
      byte invalidType = (byte) 7;

      byte[] typePlusData = new byte[1 + length];
      typePlusData[0] = invalidType;
      System.arraycopy(data, 0, typePlusData, 1, length);
      int crc = Checksum.compute(typePlusData);

      byte[] frame = new byte[WalBlockWriter.HEADER_SIZE + length];
      Coding.encodeFixed32(frame, 0, Checksum.mask(crc));
      Coding.encodeFixed16(frame, 4, length);
      frame[6] = invalidType;
      System.arraycopy(data, 0, frame, WalBlockWriter.HEADER_SIZE, length);
      Path file = dir.resolve("a.log");
      Files.write(file, frame);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("Unknown WAL record type code");
      }
    }

    @Test
    void readNext_fileTruncatedMidHeader_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write("hello".getBytes(), 1L);
      }
      // Truncate to 3 bytes (mid-header).
      byte[] raw = Files.readAllBytes(file);
      Files.write(file, java.util.Arrays.copyOf(raw, 3));

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("Truncated");
      }
    }

    @Test
    void readNext_fileTruncatedMidData_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      byte[] payload = new byte[50];
      java.util.Arrays.fill(payload, (byte) 'D');
      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write(payload, 1L);
      }
      // Truncate mid-data (keep header + a few data bytes).
      byte[] raw = Files.readAllBytes(file);
      Files.write(file, java.util.Arrays.copyOf(raw, WalBlockWriter.HEADER_SIZE + 10));

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("Truncated");
      }
    }

    @Test
    void readNext_truncatedAfterFirstFragment_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      Path file = dir.resolve("a.log");
      // A payload that splits into multiple fragments.
      byte[] payload = new byte[100000];
      java.util.Arrays.fill(payload, (byte) 'M');
      try (FileChannel ch = openNew(file)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write(payload, 1L);
      }
      // Truncate after the FIRST block only. Default 32 KB blocks → keep first 32 KB.
      byte[] raw = Files.readAllBytes(file);
      Files.write(file, java.util.Arrays.copyOf(raw, WalBlockWriter.DEFAULT_BLOCK_SIZE));

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext).isInstanceOf(CorruptionException.class);
      }
    }

    @Test
    void readNext_middleWithoutFirst_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // Hand-craft a file where the first record is MIDDLE instead of FULL or FIRST.
      Path file = dir.resolve("a.log");
      byte[] data = "bogus".getBytes();
      byte[] frame = buildFrame(WalRecordType.MIDDLE, data);
      Files.write(file, frame);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("expected FULL or FIRST");
      }
    }

    @Test
    void readNext_lengthFieldTooLarge_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // Craft a header that claims a length > (blockSize - HEADER_SIZE).
      byte[] frame = new byte[WalBlockWriter.HEADER_SIZE];
      // CRC (will be wrong but the length-check fires first)
      Coding.encodeFixed32(frame, 0, 0);
      // Length: max value (65535) — exceeds the max payload per default 32 KB block.
      Coding.encodeFixed16(frame, 4, 65535);
      frame[6] = WalRecordType.FULL.code();
      Path file = dir.resolve("a.log");
      Files.write(file, frame);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("exceeds remaining bytes in block");
      }
    }

    @Test
    void readNext_lengthCrossesBlockBoundary_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // Adversarial file: a record header placed mid-block whose length would extend across
      // the block boundary into the next block. Pre-fix the reader would silently desynchronize.
      // Use 32-byte blocks. Place a header at offset 16 that claims length 20 (16+7+20 = 43,
      // crossing into block 1 which starts at 32).
      int blockSize = 32;
      byte[] file = new byte[blockSize * 2];
      // First 16 bytes: a benign FULL record. Header at offset 0, data of length 9
      // (varint(0)=1 + payload 8). Total = 7 + 9 = 16 bytes.
      byte[] firstData = new byte[9];
      firstData[0] = 0; // varint(0)
      // Build first record's frame.
      byte[] first = buildFrame(WalRecordType.FULL, firstData);
      System.arraycopy(first, 0, file, 0, first.length);
      // Now craft the malicious second header at offset 16, claiming length 20 (would extend
      // to offset 16 + 7 + 20 = 43, crossing block 1 boundary at offset 32). CRC is bogus
      // because the cross-block check fires first.
      byte[] data = new byte[20];
      java.util.Arrays.fill(data, (byte) 'X');
      byte[] second = buildFrame(WalRecordType.FULL, data);
      // Only copy the header — actual data bytes don't matter, the length check fires first.
      System.arraycopy(second, 0, file, 16, WalBlockWriter.HEADER_SIZE);

      Path path = dir.resolve("attack.log");
      Files.write(path, file);

      try (WalBlockReader r = new WalBlockReader(path, blockSize)) {
        // First record reads cleanly.
        assertThat(r.readNext()).isPresent();
        // Second one trips the cross-block guard.
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("exceeds remaining bytes in block");
      }
    }

    @Test
    void readNext_emptyPayloadAfterSeqNoStrip_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // Hand-craft a FULL record whose data section is just a single varint byte (seqNo=42)
      // with no payload after it. The writer never emits this; the reader must reject it.
      byte[] data = new byte[1];
      data[0] = 42;
      Path file = dir.resolve("a.log");
      Files.write(file, buildFrame(WalRecordType.FULL, data));

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("no payload after stripping seqNo varint");
      }
    }

    @Test
    void readNext_malformedSeqNoVarint_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // 10 bytes all 0xFF: every continuation bit set, no terminator, varint64 parser will
      // exhaust its 64-bit shift budget and throw IAE which the reader wraps in
      // CorruptionException.
      byte[] data = new byte[10];
      java.util.Arrays.fill(data, (byte) 0xFF);
      Path file = dir.resolve("a.log");
      Files.write(file, buildFrame(WalRecordType.FULL, data));

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("Malformed seqNo varint");
      }
    }

    @Test
    void readNext_firstFollowedByFirst_throwsCorruptionException(@TempDir Path dir)
        throws IOException {
      // Hand-craft FIRST followed by another FIRST. Reader's switch only accepts MIDDLE/LAST
      // inside the multi-fragment loop; FIRST hits the default arm.
      byte[] dataA = new byte[5];
      dataA[0] = 1; // varint(1)
      byte[] dataB = new byte[5];
      dataB[0] = 2; // varint(2)
      byte[] frameA = buildFrame(WalRecordType.FIRST, dataA);
      byte[] frameB = buildFrame(WalRecordType.FIRST, dataB);
      byte[] combined = new byte[frameA.length + frameB.length];
      System.arraycopy(frameA, 0, combined, 0, frameA.length);
      System.arraycopy(frameB, 0, combined, frameA.length, frameB.length);
      Path file = dir.resolve("a.log");
      Files.write(file, combined);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("Unexpected fragment type inside multi-fragment record");
      }
    }

    @Test
    void readNext_reassembledRecordExceedsMaxRecordBytes_throwsCorruptionException(
        @TempDir Path dir) throws IOException {
      // Write a real multi-fragment record then read with a max-record cap below its size.
      Path file = dir.resolve("big.log");
      byte[] payload = new byte[100000]; // > 3 default-size blocks
      java.util.Arrays.fill(payload, (byte) 'M');
      try (FileChannel ch =
          FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        WalBlockWriter w = new WalBlockWriter(ch);
        w.write(payload, 1L);
      }

      try (WalBlockReader r = new WalBlockReader(file, WalBlockWriter.DEFAULT_BLOCK_SIZE, 1024)) {
        // The FIRST fragment is ~32 KB (blockSize - HEADER_SIZE), larger than the 1024-byte
        // cap. Pre-fix the cap only fired inside the loop, letting FIRST silently bypass it.
        // Post-fix, either the FIRST-specific error or the loop error fires — both are OK.
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageMatching("(?s).*maxRecordBytes.*");
      }
    }

    @Test
    void readNext_firstFragmentAloneExceedsMaxRecordBytes_throws(@TempDir Path dir)
        throws IOException {
      // Surgical: a single FIRST fragment whose data exceeds maxRecordBytes, verifying the
      // FIRST-specific cap check (not just the loop check). Build a FIRST frame with 20 data
      // bytes, then a LAST frame to satisfy the state machine. Set maxRecordBytes=10 so FIRST
      // trips immediately before the loop runs.
      byte[] firstData = new byte[20];
      firstData[0] = 0; // varint(0)
      for (int i = 1; i < 20; i++) {
        firstData[i] = (byte) i;
      }
      byte[] frameFirst = buildFrame(WalRecordType.FIRST, firstData);
      byte[] lastData = new byte[] {(byte) 'x'};
      byte[] frameLast = buildFrame(WalRecordType.LAST, lastData);
      byte[] combined = new byte[frameFirst.length + frameLast.length];
      System.arraycopy(frameFirst, 0, combined, 0, frameFirst.length);
      System.arraycopy(frameLast, 0, combined, frameFirst.length, frameLast.length);
      Path file = dir.resolve("first-over.log");
      Files.write(file, combined);

      try (WalBlockReader r = new WalBlockReader(file, WalBlockWriter.DEFAULT_BLOCK_SIZE, 10)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("FIRST fragment")
            .hasMessageContaining("exceeds maxRecordBytes");
      }
    }

    @Test
    void readNext_lengthZero_throwsCorruptionException(@TempDir Path dir) throws IOException {
      // Hand-craft a FULL header with length=0. Writer never emits this; reader rejects it
      // with a clear error before reaching decodeLogical (parser-differential fix).
      byte[] frame = new byte[WalBlockWriter.HEADER_SIZE];
      byte[] typeOnly = new byte[] {WalRecordType.FULL.code()};
      int crc = Checksum.compute(typeOnly);
      Coding.encodeFixed32(frame, 0, Checksum.mask(crc));
      Coding.encodeFixed16(frame, 4, 0);
      frame[6] = WalRecordType.FULL.code();

      Path file = dir.resolve("zero.log");
      Files.write(file, frame);

      try (WalBlockReader r = new WalBlockReader(file)) {
        assertThatThrownBy(r::readNext)
            .isInstanceOf(CorruptionException.class)
            .hasMessageContaining("length is zero");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static FileChannel openNew(Path file) throws IOException {
    return FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
  }

  private static byte[] buildFrame(WalRecordType type, byte[] data) {
    byte[] typePlusData = new byte[1 + data.length];
    typePlusData[0] = type.code();
    System.arraycopy(data, 0, typePlusData, 1, data.length);
    int crc = Checksum.compute(typePlusData);

    byte[] frame = new byte[WalBlockWriter.HEADER_SIZE + data.length];
    Coding.encodeFixed32(frame, 0, Checksum.mask(crc));
    Coding.encodeFixed16(frame, 4, data.length);
    frame[6] = type.code();
    System.arraycopy(data, 0, frame, WalBlockWriter.HEADER_SIZE, data.length);
    return frame;
  }
}
