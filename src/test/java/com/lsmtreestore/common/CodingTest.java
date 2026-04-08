package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link Coding}. */
class CodingTest {

  @Nested
  class FixedEncoding {

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 255, 256, 0x7FFF, 0xFFFF})
    void fixed16_roundTrip(int value) {
      byte[] buf = new byte[2];
      Coding.encodeFixed16(buf, 0, value);
      assertThat(Coding.decodeFixed16(buf, 0)).isEqualTo(value);
    }

    @Test
    void fixed16_littleEndianByteOrder() {
      byte[] buf = new byte[2];
      Coding.encodeFixed16(buf, 0, 0x0102);
      assertThat(buf[0]).isEqualTo((byte) 0x02); // low byte first
      assertThat(buf[1]).isEqualTo((byte) 0x01);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 0x7FFFFFFF, -1})
    void fixed32_roundTrip(int value) {
      byte[] buf = new byte[4];
      Coding.encodeFixed32(buf, 0, value);
      assertThat(Coding.decodeFixed32(buf, 0)).isEqualTo(value);
    }

    @Test
    void fixed32_littleEndianByteOrder() {
      byte[] buf = new byte[4];
      Coding.encodeFixed32(buf, 0, 1);
      assertThat(buf).containsExactly(0x01, 0x00, 0x00, 0x00);
    }

    @Test
    void fixed32_withOffset() {
      byte[] buf = new byte[8];
      Coding.encodeFixed32(buf, 2, 0x12345678);
      assertThat(Coding.decodeFixed32(buf, 2)).isEqualTo(0x12345678);
      // Bytes before offset should be untouched
      assertThat(buf[0]).isZero();
      assertThat(buf[1]).isZero();
    }

    static Stream<Long> fixed64Values() {
      return Stream.of(0L, 1L, 127L, Long.MAX_VALUE, -1L, 0x00000000FFFFFFFFL, 0x80000000L);
    }

    @ParameterizedTest
    @MethodSource("fixed64Values")
    void fixed64_roundTrip(long value) {
      byte[] buf = new byte[8];
      Coding.encodeFixed64(buf, 0, value);
      assertThat(Coding.decodeFixed64(buf, 0)).isEqualTo(value);
    }

    @Test
    void fixed64_littleEndianByteOrder() {
      byte[] buf = new byte[8];
      Coding.encodeFixed64(buf, 0, 1L);
      assertThat(buf).containsExactly(0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00);
    }

    @Test
    void fixed64_highBitsPreserved() {
      // Regression: decodeFixed64 must use 0xFFL, not 0xFF, to avoid sign extension
      byte[] buf = new byte[8];
      long value = 0xFF00000000000000L;
      Coding.encodeFixed64(buf, 0, value);
      assertThat(Coding.decodeFixed64(buf, 0)).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, 0x7FFFFFFF, 0x12345678})
    void fixed32_byteBuffer_roundTrip(int value) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      Coding.encodeFixed32(buf, value);
      buf.flip();
      assertThat(Coding.decodeFixed32(buf)).isEqualTo(value);
    }

    @ParameterizedTest
    @MethodSource("fixed64Values")
    void fixed64_byteBuffer_roundTrip(long value) {
      ByteBuffer buf = ByteBuffer.allocate(8);
      Coding.encodeFixed64(buf, value);
      buf.flip();
      assertThat(Coding.decodeFixed64(buf)).isEqualTo(value);
    }
  }

  @Nested
  class VarintEncoding {

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 255, 256, 16383, 16384, 0x7FFFFFFF, -1})
    void varint32_byteArray_roundTrip(int value) {
      byte[] buf = new byte[Coding.MAX_VARINT32_BYTES];
      int written = Coding.encodeVarint32(buf, 0, value);
      int[] bytesRead = new int[1];
      int decoded = Coding.decodeVarint32(buf, 0, bytesRead);
      assertThat(decoded).isEqualTo(value);
      assertThat(bytesRead[0]).isEqualTo(written);
    }

    @ParameterizedTest
    @ValueSource(
        longs = {0, 1, 127, 128, 255, 16383, 16384, 0x7FFFFFFFL, 0xFFFFFFFFL, Long.MAX_VALUE, -1})
    void varint64_byteArray_roundTrip(long value) {
      byte[] buf = new byte[Coding.MAX_VARINT64_BYTES];
      int written = Coding.encodeVarint64(buf, 0, value);
      int[] bytesRead = new int[1];
      long decoded = Coding.decodeVarint64(buf, 0, bytesRead);
      assertThat(decoded).isEqualTo(value);
      assertThat(bytesRead[0]).isEqualTo(written);
    }

    @Test
    void varint32_exactByteSequences() {
      byte[] buf = new byte[5];

      // 0 → 1 byte: 0x00
      assertThat(Coding.encodeVarint32(buf, 0, 0)).isEqualTo(1);
      assertThat(buf[0]).isEqualTo((byte) 0x00);

      // 1 → 1 byte: 0x01
      assertThat(Coding.encodeVarint32(buf, 0, 1)).isEqualTo(1);
      assertThat(buf[0]).isEqualTo((byte) 0x01);

      // 127 → 1 byte: 0x7F
      assertThat(Coding.encodeVarint32(buf, 0, 127)).isEqualTo(1);
      assertThat(buf[0]).isEqualTo((byte) 0x7F);

      // 128 → 2 bytes: 0x80, 0x01
      assertThat(Coding.encodeVarint32(buf, 0, 128)).isEqualTo(2);
      assertThat(buf[0]).isEqualTo((byte) 0x80);
      assertThat(buf[1]).isEqualTo((byte) 0x01);

      // 300 → 2 bytes: 0xAC, 0x02
      assertThat(Coding.encodeVarint32(buf, 0, 300)).isEqualTo(2);
      assertThat(buf[0]).isEqualTo((byte) 0xAC);
      assertThat(buf[1]).isEqualTo((byte) 0x02);
    }

    @Test
    void varint32_negativeOneEncodes5Bytes() {
      byte[] buf = new byte[5];
      // -1 as unsigned is 0xFFFFFFFF, needs 5 bytes
      assertThat(Coding.encodeVarint32(buf, 0, -1)).isEqualTo(5);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 255, 256, 16384, 0x7FFFFFFF, -1})
    void varint32_byteBuffer_roundTrip(int value) {
      ByteBuffer buf = ByteBuffer.allocate(Coding.MAX_VARINT32_BYTES);
      int written = Coding.encodeVarint32(buf, value);
      buf.flip();
      int decoded = Coding.decodeVarint32(buf);
      assertThat(decoded).isEqualTo(value);
      assertThat(buf.position()).isEqualTo(written);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 127, 128, 0x7FFFFFFFL, 0xFFFFFFFFL, Long.MAX_VALUE, -1})
    void varint64_byteBuffer_roundTrip(long value) {
      ByteBuffer buf = ByteBuffer.allocate(Coding.MAX_VARINT64_BYTES);
      int written = Coding.encodeVarint64(buf, value);
      buf.flip();
      long decoded = Coding.decodeVarint64(buf);
      assertThat(decoded).isEqualTo(value);
      assertThat(buf.position()).isEqualTo(written);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 255, 16384, 0x7FFFFFFF, -1})
    void varint32_outputStream_roundTrip(int value) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int written = Coding.encodeVarint32(out, value);
      byte[] encoded = out.toByteArray();
      assertThat(encoded.length).isEqualTo(written);
      int[] bytesRead = new int[1];
      int decoded = Coding.decodeVarint32(encoded, 0, bytesRead);
      assertThat(decoded).isEqualTo(value);
      assertThat(bytesRead[0]).isEqualTo(written);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 127, 128, 0xFFFFFFFFL, Long.MAX_VALUE, -1})
    void varint64_outputStream_roundTrip(long value) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      int written = Coding.encodeVarint64(out, value);
      byte[] encoded = out.toByteArray();
      assertThat(encoded.length).isEqualTo(written);
      int[] bytesRead = new int[1];
      long decoded = Coding.decodeVarint64(encoded, 0, bytesRead);
      assertThat(decoded).isEqualTo(value);
      assertThat(bytesRead[0]).isEqualTo(written);
    }
  }

  @Nested
  class VarintDecoding {

    @Test
    void decodeVarint32_malformed_throwsException() {
      // 5 continuation bytes followed by a non-continuation byte = 6 bytes total → malformed
      byte[] malformed = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01};
      int[] bytesRead = new int[1];
      assertThatThrownBy(() -> Coding.decodeVarint32(malformed, 0, bytesRead))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Malformed varint32");
    }

    @Test
    void decodeVarint64_malformed_throwsException() {
      // 10 continuation bytes followed by a non-continuation byte = 11 bytes total → malformed
      byte[] malformed = new byte[11];
      for (int i = 0; i < 10; i++) {
        malformed[i] = (byte) 0x80;
      }
      malformed[10] = 0x01;
      int[] bytesRead = new int[1];
      assertThatThrownBy(() -> Coding.decodeVarint64(malformed, 0, bytesRead))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Malformed varint64");
    }

    @Test
    void decodeVarint32_truncated_throwsException() {
      // Continuation byte with no following byte
      byte[] truncated = {(byte) 0x80};
      int[] bytesRead = new int[1];
      assertThatThrownBy(() -> Coding.decodeVarint32(truncated, 0, bytesRead))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Truncated");
    }

    @Test
    void decodeVarint64_truncated_throwsException() {
      byte[] truncated = {(byte) 0x80, (byte) 0x80};
      int[] bytesRead = new int[1];
      assertThatThrownBy(() -> Coding.decodeVarint64(truncated, 0, bytesRead))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Truncated");
    }

    @Test
    void decodeVarint32_byteBuffer_underflow_throwsException() {
      ByteBuffer buf = ByteBuffer.wrap(new byte[] {(byte) 0x80});
      assertThatThrownBy(() -> Coding.decodeVarint32(buf))
          .isInstanceOf(BufferUnderflowException.class);
    }

    @Test
    void decodeVarint64_byteBuffer_underflow_throwsException() {
      ByteBuffer buf = ByteBuffer.wrap(new byte[] {(byte) 0x80});
      assertThatThrownBy(() -> Coding.decodeVarint64(buf))
          .isInstanceOf(BufferUnderflowException.class);
    }

    @Test
    void decodeVarint32_bytesReadCorrectlyReported() {
      byte[] buf = new byte[5];
      // Encode 300 (2 bytes) at offset 0
      Coding.encodeVarint32(buf, 0, 300);
      int[] bytesRead = new int[1];
      Coding.decodeVarint32(buf, 0, bytesRead);
      assertThat(bytesRead[0]).isEqualTo(2);
    }
  }

  @Nested
  class VarintRoundTrip {

    @Test
    void multipleSequentialVarintsInOneBuffer() {
      // Simulates the BlockBuilder pattern: multiple varints packed sequentially
      byte[] buf = new byte[30];
      int pos = 0;
      pos += Coding.encodeVarint32(buf, pos, 100);
      pos += Coding.encodeVarint32(buf, pos, 200);
      pos += Coding.encodeVarint32(buf, pos, 300);
      pos += Coding.encodeVarint64(buf, pos, Long.MAX_VALUE);

      int[] bytesRead = new int[1];
      int readPos = 0;

      assertThat(Coding.decodeVarint32(buf, readPos, bytesRead)).isEqualTo(100);
      readPos += bytesRead[0];

      assertThat(Coding.decodeVarint32(buf, readPos, bytesRead)).isEqualTo(200);
      readPos += bytesRead[0];

      assertThat(Coding.decodeVarint32(buf, readPos, bytesRead)).isEqualTo(300);
      readPos += bytesRead[0];

      assertThat(Coding.decodeVarint64(buf, readPos, bytesRead)).isEqualTo(Long.MAX_VALUE);
      readPos += bytesRead[0];

      assertThat(readPos).isEqualTo(pos);
    }

    @Test
    void multipleSequentialVarintsInByteBuffer() {
      ByteBuffer buf = ByteBuffer.allocate(30);
      Coding.encodeVarint32(buf, 100);
      Coding.encodeVarint32(buf, 200);
      Coding.encodeVarint64(buf, 999999999999L);
      buf.flip();

      assertThat(Coding.decodeVarint32(buf)).isEqualTo(100);
      assertThat(Coding.decodeVarint32(buf)).isEqualTo(200);
      assertThat(Coding.decodeVarint64(buf)).isEqualTo(999999999999L);
      assertThat(buf.hasRemaining()).isFalse();
    }

    @Test
    void varint64_highBitsPreserved() {
      // Regression: (long)(b & 0x7F) must cast to long BEFORE shifting for shift >= 32
      long value = 0xFF00000000000000L;
      byte[] buf = new byte[Coding.MAX_VARINT64_BYTES];
      int written = Coding.encodeVarint64(buf, 0, value);
      int[] bytesRead = new int[1];
      long decoded = Coding.decodeVarint64(buf, 0, bytesRead);
      assertThat(decoded).isEqualTo(value);
      assertThat(bytesRead[0]).isEqualTo(written);
    }

    @Test
    void encodeDecodeWithOffset() {
      byte[] buf = new byte[20];
      // Write at offset 5
      int written = Coding.encodeVarint32(buf, 5, 12345);
      int[] bytesRead = new int[1];
      int decoded = Coding.decodeVarint32(buf, 5, bytesRead);
      assertThat(decoded).isEqualTo(12345);
      assertThat(bytesRead[0]).isEqualTo(written);
    }
  }

  @Nested
  class VarintSize {

    @Test
    void varintSize_singleByteValues() {
      assertThat(Coding.varintSize(0)).isEqualTo(1);
      assertThat(Coding.varintSize(1)).isEqualTo(1);
      assertThat(Coding.varintSize(127)).isEqualTo(1);
    }

    @Test
    void varintSize_twoByteBoundary() {
      assertThat(Coding.varintSize(128)).isEqualTo(2);
      assertThat(Coding.varintSize(16383)).isEqualTo(2);
    }

    @Test
    void varintSize_matchesActualEncoding() {
      long[] testValues = {0, 1, 127, 128, 255, 16383, 16384, Integer.MAX_VALUE, Long.MAX_VALUE};
      for (long value : testValues) {
        byte[] buf = new byte[Coding.MAX_VARINT64_BYTES];
        int actual = Coding.encodeVarint64(buf, 0, value);
        assertThat(Coding.varintSize(value)).as("varintSize(%d)", value).isEqualTo(actual);
      }
    }

    @Test
    void varintSize_maxValues() {
      // -1L as unsigned is max uint64 → 10 bytes
      assertThat(Coding.varintSize(-1L)).isEqualTo(10);
    }
  }
}
