package com.lsmtreestore.wal;

import com.lsmtreestore.common.CorruptionException;

/**
 * Type tag for a WAL record fragment in the ADR-0004 block format.
 *
 * <p>Small WAL payloads fit entirely in one {@link #FULL} record. Payloads that straddle a block
 * boundary are split into a {@link #FIRST} fragment, zero or more {@link #MIDDLE} fragments, and a
 * {@link #LAST} fragment. The on-disk byte codes (1, 2, 3, 4) are part of the WAL file format and
 * must never be renumbered — doing so would silently corrupt every WAL file written by a prior
 * version.
 */
public enum WalRecordType {
  /** The entire payload fits in this single record. */
  FULL((byte) 1),
  /** First fragment of a multi-record payload. */
  FIRST((byte) 2),
  /** Interior fragment of a multi-record payload (zero or more of these between FIRST and LAST). */
  MIDDLE((byte) 3),
  /** Final fragment of a multi-record payload. */
  LAST((byte) 4);

  private final byte code;

  WalRecordType(byte code) {
    this.code = code;
  }

  /**
   * Returns the on-disk byte code for this type.
   *
   * @return the byte written into the record header
   */
  public byte code() {
    return code;
  }

  /**
   * Decodes a byte read from a record header into its {@link WalRecordType}.
   *
   * @param code the byte read from the record's type field
   * @return the matching {@link WalRecordType}
   * @throws CorruptionException if {@code code} is not one of the four known type codes; this
   *     indicates either a corrupted record or a file produced by a future version with additional
   *     types
   */
  public static WalRecordType fromByte(byte code) throws CorruptionException {
    return switch (code) {
      case 1 -> FULL;
      case 2 -> FIRST;
      case 3 -> MIDDLE;
      case 4 -> LAST;
      default -> throw new CorruptionException("Unknown WAL record type code: " + (code & 0xFF));
    };
  }
}
