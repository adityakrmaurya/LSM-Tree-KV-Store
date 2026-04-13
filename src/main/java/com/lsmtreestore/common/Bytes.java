package com.lsmtreestore.common;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;

/**
 * Byte array utilities for the LSM Tree KV Store.
 *
 * <p>Every method is static and stateless. This class provides the canonical comparison, equality,
 * copy, hex-formatting, and prefix-length operations used throughout the engine — MemTable key
 * ordering, SSTable block encoding, WAL record writing, and more.
 *
 * <p>Hot-path methods (compare, commonPrefixLength) are designed for zero allocation and delegate
 * to JVM intrinsics where possible.
 */
public final class Bytes {

  /** Unsigned lexicographic comparator for byte arrays. Delegates to JVM-intrinsic comparison. */
  public static final Comparator<byte[]> COMPARATOR = Arrays::compareUnsigned;

  private static final HexFormat HEX = HexFormat.of();

  private Bytes() {}

  /**
   * Compares two byte arrays using unsigned lexicographic ordering.
   *
   * <p>Delegates to {@link Arrays#compareUnsigned(byte[], byte[])}, which is JVM-intrinsic on
   * modern JVMs (vectorized comparison).
   *
   * @param a first byte array
   * @param b second byte array
   * @return negative if {@code a < b}, zero if equal, positive if {@code a > b}
   */
  public static int compare(byte[] a, byte[] b) {
    return Arrays.compareUnsigned(a, b);
  }

  /**
   * Returns {@code true} if the two byte arrays have identical contents.
   *
   * <p>This is the canonical equality check for byte arrays in the codebase. Java records with
   * {@code byte[]} fields use reference equality by default, so all value equality must go through
   * this method.
   *
   * @param a first byte array
   * @param b second byte array
   * @return {@code true} if arrays are equal element-wise
   */
  public static boolean equals(byte[] a, byte[] b) {
    return Arrays.equals(a, b);
  }

  /**
   * Returns a hash code for the given byte array based on its contents.
   *
   * @param data the byte array to hash
   * @return content-based hash code
   */
  public static int hashCode(byte[] data) {
    return Arrays.hashCode(data);
  }

  /**
   * Returns a defensive copy of the given byte array.
   *
   * <p>Used for immutable key storage in {@code InternalKey} and {@code SSTableMetadata}.
   *
   * @param data the byte array to copy
   * @return a new array with identical contents
   */
  public static byte[] copy(byte[] data) {
    return Arrays.copyOf(data, data.length);
  }

  /**
   * Extracts a sub-range of the given byte array.
   *
   * <p>Used by {@code BlockReader} to extract non-shared key bytes and values.
   *
   * @param data the source byte array
   * @param offset the starting index (inclusive)
   * @param length the number of bytes to extract
   * @return a new array containing the specified range
   * @throws ArrayIndexOutOfBoundsException if the range is out of bounds
   */
  public static byte[] slice(byte[] data, int offset, int length) {
    if (offset + length > data.length) {
      throw new ArrayIndexOutOfBoundsException(
          "offset=" + offset + ", length=" + length + ", array length=" + data.length);
    }
    return Arrays.copyOfRange(data, offset, offset + length);
  }

  /**
   * Returns the full hexadecimal representation of the byte array.
   *
   * @param data the byte array to format
   * @return lowercase hex string (e.g., {@code "48656c6c6f"})
   */
  public static String toHex(byte[] data) {
    return HEX.formatHex(data);
  }

  /**
   * Returns a truncated debug string suitable for logging.
   *
   * <p>Format: {@code "[length] hexprefix..."} — prevents log spam for large values.
   *
   * @param data the byte array to format
   * @param maxBytes maximum number of bytes to include in the hex portion
   * @return debug string with length prefix and optional truncation
   */
  public static String toDebugString(byte[] data, int maxBytes) {
    if (data.length <= maxBytes) {
      return "[" + data.length + "] " + HEX.formatHex(data);
    }
    return "[" + data.length + "] " + HEX.formatHex(data, 0, maxBytes) + "...";
  }

  /**
   * Computes the length of the common prefix between two byte arrays.
   *
   * <p>Called by {@code BlockBuilder} on every entry for prefix compression. Simple byte-by-byte
   * loop that is JIT-friendly.
   *
   * @param a first byte array
   * @param b second byte array
   * @return number of leading bytes that are identical
   */
  public static int commonPrefixLength(byte[] a, byte[] b) {
    int limit = Math.min(a.length, b.length);
    int i = 0;
    while (i < limit && a[i] == b[i]) {
      i++;
    }
    return i;
  }
}
