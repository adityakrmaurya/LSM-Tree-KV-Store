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
            .hasMessageContaining("exceeds max payload");
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
