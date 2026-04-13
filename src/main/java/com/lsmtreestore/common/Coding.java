package com.lsmtreestore.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Fixed-size and variable-length integer encoding utilities.
 *
 * <p>All multi-byte integers use <strong>little-endian</strong> byte order, matching LevelDB and
 * Protobuf conventions. Varint encoding uses 7 data bits per byte with the MSB as a continuation
 * flag.
 *
 * <p>This class is on the hottest paths in the engine — every block entry encode/decode, every WAL
 * record header, every varint read. Implementations are zero-allocation where possible, using
 * direct bit-shift operations on {@code byte[]} targets instead of allocating {@link ByteBuffer}
 * wrappers.
 */
public final class Coding {

  /** Maximum number of bytes a varint32 can occupy (ceil(32/7)). */
  public static final int MAX_VARINT32_BYTES = 5;

  /** Maximum number of bytes a varint64 can occupy (ceil(64/7)). */
  public static final int MAX_VARINT64_BYTES = 10;

  private Coding() {}

  // ---------------------------------------------------------------------------
  // Fixed-size little-endian encoding
  // ---------------------------------------------------------------------------

  /**
   * Encodes a 16-bit integer in little-endian format into the destination array.
   *
   * @param dst destination byte array
   * @param offset starting position in {@code dst}
   * @param value the 16-bit value to encode (only lower 16 bits are used)
   */
  public static void encodeFixed16(byte[] dst, int offset, int value) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
  }

  /**
   * Encodes a 32-bit integer in little-endian format into the destination array.
   *
   * @param dst destination byte array
   * @param offset starting position in {@code dst}
   * @param value the 32-bit value to encode
   */
  public static void encodeFixed32(byte[] dst, int offset, int value) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
    dst[offset + 2] = (byte) (value >>> 16);
    dst[offset + 3] = (byte) (value >>> 24);
  }

  /**
   * Encodes a 32-bit integer in little-endian format into the buffer at its current position.
   *
   * <p>Writes bytes manually to avoid depending on buffer byte order.
   *
   * @param buf the target buffer (position advances by 4)
   * @param value the 32-bit value to encode
   */
  public static void encodeFixed32(ByteBuffer buf, int value) {
    buf.put((byte) value);
    buf.put((byte) (value >>> 8));
    buf.put((byte) (value >>> 16));
    buf.put((byte) (value >>> 24));
  }

  /**
   * Encodes a 64-bit integer in little-endian format into the destination array.
   *
   * @param dst destination byte array
   * @param offset starting position in {@code dst}
   * @param value the 64-bit value to encode
   */
  public static void encodeFixed64(byte[] dst, int offset, long value) {
    dst[offset] = (byte) value;
    dst[offset + 1] = (byte) (value >>> 8);
    dst[offset + 2] = (byte) (value >>> 16);
    dst[offset + 3] = (byte) (value >>> 24);
    dst[offset + 4] = (byte) (value >>> 32);
    dst[offset + 5] = (byte) (value >>> 40);
    dst[offset + 6] = (byte) (value >>> 48);
    dst[offset + 7] = (byte) (value >>> 56);
  }

  /**
   * Encodes a 64-bit integer in little-endian format into the buffer at its current position.
   *
   * @param buf the target buffer (position advances by 8)
   * @param value the 64-bit value to encode
   */
  public static void encodeFixed64(ByteBuffer buf, long value) {
    buf.put((byte) value);
    buf.put((byte) (value >>> 8));
    buf.put((byte) (value >>> 16));
    buf.put((byte) (value >>> 24));
    buf.put((byte) (value >>> 32));
    buf.put((byte) (value >>> 40));
    buf.put((byte) (value >>> 48));
    buf.put((byte) (value >>> 56));
  }

  // ---------------------------------------------------------------------------
  // Fixed-size little-endian decoding
  // ---------------------------------------------------------------------------

  /**
   * Decodes an unsigned 16-bit little-endian integer from the source array.
   *
   * @param src source byte array
   * @param offset starting position in {@code src}
   * @return the decoded unsigned 16-bit value (0–65535)
   */
  public static int decodeFixed16(byte[] src, int offset) {
    return (src[offset] & 0xFF) | ((src[offset + 1] & 0xFF) << 8);
  }

  /**
   * Decodes a 32-bit little-endian integer from the source array.
   *
   * @param src source byte array
   * @param offset starting position in {@code src}
   * @return the decoded 32-bit value
   */
  public static int decodeFixed32(byte[] src, int offset) {
    return (src[offset] & 0xFF)
        | ((src[offset + 1] & 0xFF) << 8)
        | ((src[offset + 2] & 0xFF) << 16)
        | ((src[offset + 3] & 0xFF) << 24);
  }

  /**
   * Decodes a 32-bit little-endian integer from the buffer at its current position.
   *
   * @param buf the source buffer (position advances by 4)
   * @return the decoded 32-bit value
   */
  public static int decodeFixed32(ByteBuffer buf) {
    return (buf.get() & 0xFF)
        | ((buf.get() & 0xFF) << 8)
        | ((buf.get() & 0xFF) << 16)
        | ((buf.get() & 0xFF) << 24);
  }

  /**
   * Decodes a 64-bit little-endian integer from the source array.
   *
   * <p><strong>Critical</strong>: Uses {@code 0xFFL} mask (not {@code 0xFF}) to prevent sign
   * extension when bytes at positions 4–7 have values &ge; 0x80.
   *
   * @param src source byte array
   * @param offset starting position in {@code src}
   * @return the decoded 64-bit value
   */
  public static long decodeFixed64(byte[] src, int offset) {
    return (src[offset] & 0xFFL)
        | ((src[offset + 1] & 0xFFL) << 8)
        | ((src[offset + 2] & 0xFFL) << 16)
        | ((src[offset + 3] & 0xFFL) << 24)
        | ((src[offset + 4] & 0xFFL) << 32)
        | ((src[offset + 5] & 0xFFL) << 40)
        | ((src[offset + 6] & 0xFFL) << 48)
        | ((src[offset + 7] & 0xFFL) << 56);
  }

  /**
   * Decodes a 64-bit little-endian integer from the buffer at its current position.
   *
   * @param buf the source buffer (position advances by 8)
   * @return the decoded 64-bit value
   */
  public static long decodeFixed64(ByteBuffer buf) {
    return (buf.get() & 0xFFL)
        | ((buf.get() & 0xFFL) << 8)
        | ((buf.get() & 0xFFL) << 16)
        | ((buf.get() & 0xFFL) << 24)
        | ((buf.get() & 0xFFL) << 32)
        | ((buf.get() & 0xFFL) << 40)
        | ((buf.get() & 0xFFL) << 48)
        | ((buf.get() & 0xFFL) << 56);
  }

  // ---------------------------------------------------------------------------
  // Varint encoding
  // ---------------------------------------------------------------------------

  /**
   * Encodes a 32-bit integer as a varint into the destination array.
   *
   * <p>Uses {@link Integer#toUnsignedLong(int)} for the loop condition to correctly handle negative
   * int values (which represent large unsigned 32-bit values).
   *
   * @param dst destination byte array
   * @param offset starting position in {@code dst}
   * @param value the 32-bit value to encode
   * @return the number of bytes written (1–5)
   */
  public static int encodeVarint32(byte[] dst, int offset, int value) {
    long unsigned = Integer.toUnsignedLong(value);
    int pos = offset;
    while (unsigned > 0x7F) {
      dst[pos++] = (byte) ((unsigned & 0x7F) | 0x80);
      unsigned >>>= 7;
    }
    dst[pos++] = (byte) unsigned;
    return pos - offset;
  }

  /**
   * Encodes a 32-bit integer as a varint into the buffer at its current position.
   *
   * @param buf the target buffer (position advances by 1–5)
   * @param value the 32-bit value to encode
   * @return the number of bytes written (1–5)
   */
  public static int encodeVarint32(ByteBuffer buf, int value) {
    long unsigned = Integer.toUnsignedLong(value);
    int written = 0;
    while (unsigned > 0x7F) {
      buf.put((byte) ((unsigned & 0x7F) | 0x80));
      unsigned >>>= 7;
      written++;
    }
    buf.put((byte) unsigned);
    written++;
    return written;
  }

  /**
   * Encodes a 32-bit integer as a varint to the output stream.
   *
   * <p>Used by {@code BlockBuilder} which writes to a {@code ByteArrayOutputStream}.
   *
   * @param out the target output stream
   * @param value the 32-bit value to encode
   * @return the number of bytes written (1–5)
   * @throws IOException if an I/O error occurs
   */
  public static int encodeVarint32(OutputStream out, int value) throws IOException {
    long unsigned = Integer.toUnsignedLong(value);
    int written = 0;
    while (unsigned > 0x7F) {
      out.write((int) ((unsigned & 0x7F) | 0x80));
      unsigned >>>= 7;
      written++;
    }
    out.write((int) unsigned);
    written++;
    return written;
  }

  /**
   * Encodes a 64-bit integer as a varint into the destination array.
   *
   * @param dst destination byte array
   * @param offset starting position in {@code dst}
   * @param value the 64-bit value to encode
   * @return the number of bytes written (1–10)
   */
  public static int encodeVarint64(byte[] dst, int offset, long value) {
    int pos = offset;
    while (Long.compareUnsigned(value, 0x7F) > 0) {
      dst[pos++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    dst[pos++] = (byte) value;
    return pos - offset;
  }

  /**
   * Encodes a 64-bit integer as a varint into the buffer at its current position.
   *
   * @param buf the target buffer (position advances by 1–10)
   * @param value the 64-bit value to encode
   * @return the number of bytes written (1–10)
   */
  public static int encodeVarint64(ByteBuffer buf, long value) {
    int written = 0;
    while (Long.compareUnsigned(value, 0x7F) > 0) {
      buf.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
      written++;
    }
    buf.put((byte) value);
    written++;
    return written;
  }

  /**
   * Encodes a 64-bit integer as a varint to the output stream.
   *
   * @param out the target output stream
   * @param value the 64-bit value to encode
   * @return the number of bytes written (1–10)
   * @throws IOException if an I/O error occurs
   */
  public static int encodeVarint64(OutputStream out, long value) throws IOException {
    int written = 0;
    while (Long.compareUnsigned(value, 0x7F) > 0) {
      out.write((int) ((value & 0x7F) | 0x80));
      value >>>= 7;
      written++;
    }
    out.write((int) value);
    written++;
    return written;
  }

  // ---------------------------------------------------------------------------
  // Varint decoding
  // ---------------------------------------------------------------------------

  /**
   * Decodes a varint32 from the source array.
   *
   * @param src source byte array
   * @param offset starting position in {@code src}
   * @param bytesRead single-element array that receives the number of bytes consumed (1–5). Callers
   *     on hot paths reuse a single {@code int[1]} to avoid allocation.
   * @return the decoded 32-bit value
   * @throws IllegalArgumentException if the varint is malformed (too many continuation bytes) or
   *     the buffer is truncated
   */
  public static int decodeVarint32(byte[] src, int offset, int[] bytesRead) {
    int result = 0;
    int shift = 0;
    int pos = offset;
    while (shift < 32) {
      if (pos >= src.length) {
        throw new IllegalArgumentException(
            "Truncated varint32 at offset " + offset + " (read " + (pos - offset) + " bytes)");
      }
      byte b = src[pos++];
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        bytesRead[0] = pos - offset;
        return result;
      }
      shift += 7;
    }
    throw new IllegalArgumentException(
        "Malformed varint32 at offset " + offset + ": too many continuation bytes");
  }

  /**
   * Decodes a varint32 from the buffer at its current position.
   *
   * @param buf the source buffer (position advances by the number of bytes consumed)
   * @return the decoded 32-bit value
   * @throws IllegalArgumentException if the varint is malformed (too many continuation bytes)
   */
  public static int decodeVarint32(ByteBuffer buf) {
    int result = 0;
    int shift = 0;
    while (shift < 32) {
      byte b = buf.get();
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalArgumentException("Malformed varint32: too many continuation bytes");
  }

  /**
   * Decodes a varint64 from the source array.
   *
   * <p><strong>Critical</strong>: The expression {@code (long)(b & 0x7F) << shift} casts to {@code
   * long} before the shift. Without this, for {@code shift >= 32} the shift operates on {@code int}
   * and loses high bits.
   *
   * @param src source byte array
   * @param offset starting position in {@code src}
   * @param bytesRead single-element array that receives the number of bytes consumed (1–10)
   * @return the decoded 64-bit value
   * @throws IllegalArgumentException if the varint is malformed or the buffer is truncated
   */
  public static long decodeVarint64(byte[] src, int offset, int[] bytesRead) {
    long result = 0;
    int shift = 0;
    int pos = offset;
    while (shift < 64) {
      if (pos >= src.length) {
        throw new IllegalArgumentException(
            "Truncated varint64 at offset " + offset + " (read " + (pos - offset) + " bytes)");
      }
      byte b = src[pos++];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        bytesRead[0] = pos - offset;
        return result;
      }
      shift += 7;
    }
    throw new IllegalArgumentException(
        "Malformed varint64 at offset " + offset + ": too many continuation bytes");
  }

  /**
   * Decodes a varint64 from the buffer at its current position.
   *
   * @param buf the source buffer (position advances by the number of bytes consumed)
   * @return the decoded 64-bit value
   * @throws IllegalArgumentException if the varint is malformed (too many continuation bytes)
   */
  public static long decodeVarint64(ByteBuffer buf) {
    long result = 0;
    int shift = 0;
    while (shift < 64) {
      byte b = buf.get();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new IllegalArgumentException("Malformed varint64: too many continuation bytes");
  }

  // ---------------------------------------------------------------------------
  // Varint size helper
  // ---------------------------------------------------------------------------

  /**
   * Returns the number of bytes needed to encode the given value as a varint.
   *
   * <p>Used by {@code BlockBuilder.estimatedSize()} and {@code Footer.encode()} to pre-compute
   * buffer sizes.
   *
   * @param value the value to measure (treated as unsigned)
   * @return the encoded size in bytes (1–10)
   */
  public static int varintSize(long value) {
    int size = 1;
    while (Long.compareUnsigned(value, 0x7F) > 0) {
      value >>>= 7;
      size++;
    }
    return size;
  }
}
