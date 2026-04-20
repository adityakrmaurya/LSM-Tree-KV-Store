package com.lsmtreestore.wal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link WalRecord} equality, hashing, and string rendering. */
class WalRecordTest {

  private static final byte[] HELLO = {'h', 'e', 'l', 'l', 'o'};
  private static final byte[] HELLO_COPY = {'h', 'e', 'l', 'l', 'o'};
  private static final byte[] WORLD = {'w', 'o', 'r', 'l', 'd'};

  @Nested
  class Equality {

    @Test
    void equals_samePayloadBytesAndSeqNoAndType_returnsTrue() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(HELLO_COPY, 1L, WalRecordType.FULL);
      assertThat(a).isEqualTo(b);
    }

    @Test
    void equals_differentPayload_returnsFalse() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(WORLD, 1L, WalRecordType.FULL);
      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equals_differentSeqNo_returnsFalse() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(HELLO, 2L, WalRecordType.FULL);
      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equals_differentType_returnsFalse() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(HELLO, 1L, WalRecordType.FIRST);
      assertThat(a).isNotEqualTo(b);
    }

    @Test
    void equals_nonWalRecordObject_returnsFalse() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      assertThat(a).isNotEqualTo("not a WalRecord");
    }

    @Test
    void equals_null_returnsFalse() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      assertThat(a).isNotEqualTo(null);
    }

    @Test
    void equals_self_returnsTrue() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      assertThat(a).isEqualTo(a);
    }
  }

  @Nested
  class HashCode {

    @Test
    void hashCode_equalRecords_haveEqualHashCodes() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(HELLO_COPY, 1L, WalRecordType.FULL);
      assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void hashCode_differentPayloads_haveDifferentHashCodesUsually() {
      // Not a strict contract — hash collisions are legal — but for obviously different inputs
      // the AssertJ default hash of payloadBytes should distinguish these two records.
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      WalRecord b = new WalRecord(WORLD, 1L, WalRecordType.FULL);
      assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
    }
  }

  @Nested
  class ToString {

    @Test
    void toString_includesSeqNoAndType() {
      WalRecord a = new WalRecord(HELLO, 42L, WalRecordType.FIRST);
      assertThat(a.toString()).contains("seqNo=42").contains("type=FIRST");
    }

    @Test
    void toString_includesPayloadLength() {
      WalRecord a = new WalRecord(HELLO, 1L, WalRecordType.FULL);
      assertThat(a.toString()).contains("payload.length=" + HELLO.length);
    }

    @Test
    void toString_nullPayload_rendersNull() {
      WalRecord a = new WalRecord(null, 1L, WalRecordType.FULL);
      assertThat(a.toString()).contains("payload.length=null");
    }
  }
}
