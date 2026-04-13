package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link Bytes}. */
class BytesTest {

  @Nested
  class Compare {

    @Test
    void compare_equalArrays_returnsZero() {
      byte[] a = {1, 2, 3};
      byte[] b = {1, 2, 3};
      assertThat(Bytes.compare(a, b)).isZero();
    }

    @Test
    void compare_firstSmaller_returnsNegative() {
      byte[] a = {1, 2, 3};
      byte[] b = {1, 2, 4};
      assertThat(Bytes.compare(a, b)).isNegative();
    }

    @Test
    void compare_firstLarger_returnsPositive() {
      byte[] a = {1, 2, 4};
      byte[] b = {1, 2, 3};
      assertThat(Bytes.compare(a, b)).isPositive();
    }

    @Test
    void compare_unsignedSemantics_ffGreaterThanZero() {
      // 0xFF is -1 as signed byte, but 255 as unsigned — must compare greater than 0x00
      byte[] a = {(byte) 0xFF};
      byte[] b = {0x00};
      assertThat(Bytes.compare(a, b)).isPositive();
    }

    @Test
    void compare_differentLengths_shorterIsSmaller() {
      byte[] a = {1, 2};
      byte[] b = {1, 2, 3};
      assertThat(Bytes.compare(a, b)).isNegative();
    }

    @Test
    void compare_emptyArrays_returnsZero() {
      assertThat(Bytes.compare(new byte[0], new byte[0])).isZero();
    }

    @Test
    void compare_emptyVsNonEmpty_emptyIsSmaller() {
      assertThat(Bytes.compare(new byte[0], new byte[] {1})).isNegative();
    }

    @Test
    void comparator_sortsCorrectly() {
      List<byte[]> list =
          new ArrayList<>(
              List.of(new byte[] {3}, new byte[] {1}, new byte[] {2}, new byte[] {(byte) 0xFF}));
      list.sort(Bytes.COMPARATOR);
      assertThat(Bytes.compare(list.get(0), new byte[] {1})).isZero();
      assertThat(Bytes.compare(list.get(1), new byte[] {2})).isZero();
      assertThat(Bytes.compare(list.get(2), new byte[] {3})).isZero();
      assertThat(Bytes.compare(list.get(3), new byte[] {(byte) 0xFF})).isZero();
    }
  }

  @Nested
  class Equality {

    @Test
    void equals_sameContents_returnsTrue() {
      assertThat(Bytes.equals(new byte[] {1, 2, 3}, new byte[] {1, 2, 3})).isTrue();
    }

    @Test
    void equals_differentContents_returnsFalse() {
      assertThat(Bytes.equals(new byte[] {1, 2, 3}, new byte[] {1, 2, 4})).isFalse();
    }

    @Test
    void equals_differentLengths_returnsFalse() {
      assertThat(Bytes.equals(new byte[] {1, 2}, new byte[] {1, 2, 3})).isFalse();
    }

    @Test
    void hashCode_sameContents_sameHash() {
      assertThat(Bytes.hashCode(new byte[] {1, 2, 3}))
          .isEqualTo(Bytes.hashCode(new byte[] {1, 2, 3}));
    }

    @Test
    void hashCode_differentContents_likelyDifferentHash() {
      assertThat(Bytes.hashCode(new byte[] {1, 2, 3}))
          .isNotEqualTo(Bytes.hashCode(new byte[] {4, 5, 6}));
    }
  }

  @Nested
  class CopyAndSlice {

    @Test
    void copy_returnsDefensiveCopy() {
      byte[] original = {1, 2, 3};
      byte[] copied = Bytes.copy(original);
      original[0] = 99;
      assertThat(copied).containsExactly(1, 2, 3);
    }

    @Test
    void copy_returnsSameContents() {
      byte[] data = {10, 20, 30};
      assertThat(Bytes.copy(data)).containsExactly(10, 20, 30);
    }

    @Test
    void slice_extractsRange() {
      byte[] data = {10, 20, 30, 40, 50};
      assertThat(Bytes.slice(data, 1, 3)).containsExactly(20, 30, 40);
    }

    @Test
    void slice_outOfBounds_throwsException() {
      byte[] data = {1, 2, 3};
      assertThatThrownBy(() -> Bytes.slice(data, 1, 5))
          .isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }
  }

  @Nested
  class Hex {

    @Test
    void toHex_knownBytes_returnsExpected() {
      assertThat(Bytes.toHex(new byte[] {0x48, 0x65, 0x6c, 0x6c, 0x6f})).isEqualTo("48656c6c6f");
    }

    @Test
    void toHex_emptyArray_returnsEmptyString() {
      assertThat(Bytes.toHex(new byte[0])).isEmpty();
    }

    @Test
    void toDebugString_shortArray_noTruncation() {
      byte[] data = {0x01, 0x02, 0x03};
      assertThat(Bytes.toDebugString(data, 10)).isEqualTo("[3] 010203");
    }

    @Test
    void toDebugString_longArray_truncated() {
      byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
      String result = Bytes.toDebugString(data, 2);
      assertThat(result).isEqualTo("[5] 0102...");
    }

    @Test
    void toDebugString_exactMaxBytes_noTruncation() {
      byte[] data = {0x0A, 0x0B};
      assertThat(Bytes.toDebugString(data, 2)).isEqualTo("[2] 0a0b");
    }
  }

  @Nested
  class CommonPrefixLength {

    @Test
    void commonPrefixLength_noSharedPrefix_returnsZero() {
      assertThat(Bytes.commonPrefixLength(new byte[] {1}, new byte[] {2})).isZero();
    }

    @Test
    void commonPrefixLength_fullMatch_returnsLength() {
      byte[] data = {1, 2, 3};
      assertThat(Bytes.commonPrefixLength(data, data.clone())).isEqualTo(3);
    }

    @Test
    void commonPrefixLength_partialMatch_returnsSharedLength() {
      assertThat(Bytes.commonPrefixLength(new byte[] {1, 2, 3}, new byte[] {1, 2, 99}))
          .isEqualTo(2);
    }

    @Test
    void commonPrefixLength_emptyArray_returnsZero() {
      assertThat(Bytes.commonPrefixLength(new byte[0], new byte[] {1})).isZero();
    }

    @Test
    void commonPrefixLength_differentLengths_matchesUpToShorter() {
      assertThat(Bytes.commonPrefixLength(new byte[] {1, 2}, new byte[] {1, 2, 3})).isEqualTo(2);
    }
  }
}
