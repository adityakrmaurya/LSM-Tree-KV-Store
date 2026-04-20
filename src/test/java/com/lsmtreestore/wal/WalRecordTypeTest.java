package com.lsmtreestore.wal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lsmtreestore.common.CorruptionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link WalRecordType}. */
class WalRecordTypeTest {

  @Nested
  class Code {

    @Test
    void code_full_returns1() {
      assertThat(WalRecordType.FULL.code()).isEqualTo((byte) 1);
    }

    @Test
    void code_first_returns2() {
      assertThat(WalRecordType.FIRST.code()).isEqualTo((byte) 2);
    }

    @Test
    void code_middle_returns3() {
      assertThat(WalRecordType.MIDDLE.code()).isEqualTo((byte) 3);
    }

    @Test
    void code_last_returns4() {
      assertThat(WalRecordType.LAST.code()).isEqualTo((byte) 4);
    }
  }

  @Nested
  class FromByte {

    @Test
    void fromByte_codeOne_returnsFull() {
      assertThat(WalRecordType.fromByte((byte) 1)).isEqualTo(WalRecordType.FULL);
    }

    @Test
    void fromByte_codeTwo_returnsFirst() {
      assertThat(WalRecordType.fromByte((byte) 2)).isEqualTo(WalRecordType.FIRST);
    }

    @Test
    void fromByte_codeThree_returnsMiddle() {
      assertThat(WalRecordType.fromByte((byte) 3)).isEqualTo(WalRecordType.MIDDLE);
    }

    @Test
    void fromByte_codeFour_returnsLast() {
      assertThat(WalRecordType.fromByte((byte) 4)).isEqualTo(WalRecordType.LAST);
    }

    @Test
    void fromByte_zero_throwsCorruptionException() {
      assertThatThrownBy(() -> WalRecordType.fromByte((byte) 0))
          .isInstanceOf(CorruptionException.class)
          .hasMessageContaining("Unknown WAL record type code: 0");
    }

    @Test
    void fromByte_negativeOne_throwsCorruptionExceptionWithUnsignedValue() {
      // 0xFF as a byte is -1 signed; the error message should show the unsigned value 255.
      assertThatThrownBy(() -> WalRecordType.fromByte((byte) 0xFF))
          .isInstanceOf(CorruptionException.class)
          .hasMessageContaining("255");
    }

    @Test
    void fromByte_codeFive_throwsCorruptionException() {
      assertThatThrownBy(() -> WalRecordType.fromByte((byte) 5))
          .isInstanceOf(CorruptionException.class);
    }
  }
}
