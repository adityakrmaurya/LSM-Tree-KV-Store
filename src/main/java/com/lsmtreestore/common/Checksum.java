package com.lsmtreestore.common;

import java.util.zip.CRC32C;

/**
 * CRC32C checksum utility for data integrity verification.
 *
 * <p>Uses {@link java.util.zip.CRC32C} which is hardware-accelerated on modern x86 and ARM
 * processors via the SSE 4.2 CRC32 instruction. This class provides the canonical checksum
 * operations used by both the WAL (record checksums) and SSTable (block checksums) subsystems.
 *
 * <p>Masking prevents a stored checksum from accidentally matching a CRC computed over data that
 * includes the checksum itself. The mask formula matches LevelDB's convention.
 */
public final class Checksum {

  /** Mask delta constant from LevelDB. Added during masking to further scramble the value. */
  private static final int MASK_DELTA = 0xa282ead8;

  private Checksum() {}

  /**
   * Computes the CRC32C checksum of the given data.
   *
   * @param data the byte array to checksum
   * @return the CRC32C value as a 32-bit integer
   */
  public static int compute(byte[] data) {
    return compute(data, 0, data.length);
  }

  /**
   * Computes the CRC32C checksum of a portion of the given data.
   *
   * @param data the source byte array
   * @param offset the starting position
   * @param length the number of bytes to include
   * @return the CRC32C value as a 32-bit integer
   */
  public static int compute(byte[] data, int offset, int length) {
    CRC32C crc32c = new CRC32C();
    crc32c.update(data, offset, length);
    return (int) crc32c.getValue();
  }

  /**
   * Verifies that the given data matches the expected checksum.
   *
   * @param data the byte array to verify
   * @param expectedChecksum the expected CRC32C value
   * @return {@code true} if the computed checksum matches the expected value
   */
  public static boolean verify(byte[] data, int expectedChecksum) {
    return compute(data) == expectedChecksum;
  }

  /**
   * Masks a checksum for storage.
   *
   * <p>Prevents the CRC of a block from accidentally matching a CRC stored within the block.
   * Formula: {@code ((crc >>> 15) | (crc << 17)) + MASK_DELTA}.
   *
   * @param checksum the raw CRC32C value
   * @return the masked checksum
   */
  public static int mask(int checksum) {
    return ((checksum >>> 15) | (checksum << 17)) + MASK_DELTA;
  }

  /**
   * Unmasks a previously masked checksum.
   *
   * <p>Reverses the {@link #mask(int)} operation to recover the original CRC32C value.
   *
   * @param maskedChecksum the masked checksum
   * @return the original CRC32C value
   */
  public static int unmask(int maskedChecksum) {
    int rotated = maskedChecksum - MASK_DELTA;
    return ((rotated >>> 17) | (rotated << 15));
  }
}
