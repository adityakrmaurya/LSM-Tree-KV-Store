package com.lsmtreestore.wal;

import java.util.Arrays;
import java.util.Objects;

/**
 * A reassembled WAL record as surfaced by {@link WalBlockReader}.
 *
 * <p>This is the caller-visible shape: a single logical payload plus its assigned sequence number
 * and the fragment type that carried it (always {@link WalRecordType#FULL} for single-record
 * payloads; for multi-fragment payloads the reader reassembles the fragments and reports {@link
 * WalRecordType#FULL} as the logical type — the on-disk FIRST/MIDDLE/LAST split is an
 * implementation detail of the block format, not a caller concern).
 *
 * <p>Equality compares the payload bytes deeply ({@link Arrays#equals(byte[], byte[])}) rather than
 * by array reference, which is the contract testers expect from a value type. The {@code byte[]}
 * field is defensively <strong>not</strong> copied on construction — callers are trusted to treat
 * records as immutable.
 *
 * @param payload the logical payload bytes (the byte[] handed to {@link WalBlockWriter#write})
 * @param seqNo the sequence number assigned by the coordinator when the payload was appended
 * @param type the fragment type; always {@link WalRecordType#FULL} when surfaced from the reader
 */
public record WalRecord(byte[] payload, long seqNo, WalRecordType type) {

  @Override
  public boolean equals(Object o) {
    return o instanceof WalRecord other
        && seqNo == other.seqNo
        && type == other.type
        && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Arrays.hashCode(payload), seqNo, type);
  }

  @Override
  public String toString() {
    return "WalRecord[seqNo="
        + seqNo
        + ", type="
        + type
        + ", payload.length="
        + (payload == null ? "null" : payload.length)
        + "]";
  }
}
