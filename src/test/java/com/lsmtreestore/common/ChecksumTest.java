package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link Checksum}. */
class ChecksumTest {

  @Nested
  class Compute {

    @Test
    void compute_knownInput_returnsConsistentChecksum() {
      byte[] data = "hello".getBytes();
      int crc1 = Checksum.compute(data);
      int crc2 = Checksum.compute(data);
      assertThat(crc1).isEqualTo(crc2);
    }

    @Test
    void compute_differentInputs_returnDifferentChecksums() {
      int crc1 = Checksum.compute("hello".getBytes());
      int crc2 = Checksum.compute("world".getBytes());
      assertThat(crc1).isNotEqualTo(crc2);
    }

    @Test
    void compute_emptyArray_returnsValidChecksum() {
      int crc = Checksum.compute(new byte[0]);
      // CRC32C of empty input is 0
      assertThat(crc).isEqualTo(0);
    }

    @Test
    void compute_withOffsetAndLength_checksSubarray() {
      byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
      int crcFull = Checksum.compute(new byte[] {0x02, 0x03, 0x04});
      int crcSlice = Checksum.compute(data, 1, 3);
      assertThat(crcSlice).isEqualTo(crcFull);
    }

    @Test
    void compute_singleByte_returnsNonZero() {
      int crc = Checksum.compute(new byte[] {0x42});
      assertThat(crc).isNotZero();
    }
  }

  @Nested
  class Verify {

    @Test
    void verify_correctChecksum_returnsTrue() {
      byte[] data = "test data".getBytes();
      int checksum = Checksum.compute(data);
      assertThat(Checksum.verify(data, checksum)).isTrue();
    }

    @Test
    void verify_wrongChecksum_returnsFalse() {
      byte[] data = "test data".getBytes();
      int checksum = Checksum.compute(data);
      assertThat(Checksum.verify(data, checksum + 1)).isFalse();
    }

    @Test
    void verify_corruptedData_returnsFalse() {
      byte[] data = "original".getBytes();
      int checksum = Checksum.compute(data);
      data[0] = (byte) 0xFF; // corrupt one byte
      assertThat(Checksum.verify(data, checksum)).isFalse();
    }

    @Test
    void verify_emptyArray_withCorrectChecksum_returnsTrue() {
      byte[] data = new byte[0];
      int checksum = Checksum.compute(data);
      assertThat(Checksum.verify(data, checksum)).isTrue();
    }
  }

  @Nested
  class MaskUnmask {

    @Test
    void maskUnmask_roundTrip_returnsOriginal() {
      int original = Checksum.compute("round trip".getBytes());
      int masked = Checksum.mask(original);
      int unmasked = Checksum.unmask(masked);
      assertThat(unmasked).isEqualTo(original);
    }

    @Test
    void mask_changesValue() {
      int original = Checksum.compute("mask test".getBytes());
      int masked = Checksum.mask(original);
      assertThat(masked).isNotEqualTo(original);
    }

    @Test
    void maskUnmask_zero_roundTrips() {
      int masked = Checksum.mask(0);
      assertThat(Checksum.unmask(masked)).isZero();
    }

    @Test
    void maskUnmask_maxInt_roundTrips() {
      int masked = Checksum.mask(Integer.MAX_VALUE);
      assertThat(Checksum.unmask(masked)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void maskUnmask_negativeInt_roundTrips() {
      // CRC values can be any 32-bit pattern including negative signed int
      int masked = Checksum.mask(-1);
      assertThat(Checksum.unmask(masked)).isEqualTo(-1);
    }
  }
}
