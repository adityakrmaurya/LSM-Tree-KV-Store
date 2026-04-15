package com.lsmtreestore.common;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Exception for data integrity violations.
 *
 * <p>Thrown when the storage engine detects corruption such as a bad CRC checksum, truncated file,
 * invalid magic number, or malformed varint. Optionally includes the {@link Path} and byte offset
 * where corruption was detected.
 */
public class CorruptionException extends StoreException {

  private final Path path;
  private final long offset;
  private final boolean hasOffset;

  /**
   * Creates a corruption exception with the given message.
   *
   * @param message description of the corruption
   */
  public CorruptionException(String message) {
    super(message);
    this.path = null;
    this.offset = -1;
    this.hasOffset = false;
  }

  /**
   * Creates a corruption exception with file location details.
   *
   * @param message description of the corruption
   * @param path the file where corruption was detected
   * @param offset the byte offset within the file
   */
  public CorruptionException(String message, Path path, long offset) {
    super(message + " [path=" + path + ", offset=" + offset + "]");
    this.path = path;
    this.offset = offset;
    this.hasOffset = true;
  }

  /**
   * Returns the file path where corruption was detected, if available.
   *
   * @return an {@link Optional} containing the path, or empty
   */
  public Optional<Path> getPath() {
    return Optional.ofNullable(path);
  }

  /**
   * Returns the byte offset where corruption was detected, if available.
   *
   * @return an {@link OptionalLong} containing the offset, or empty
   */
  public OptionalLong getOffset() {
    return hasOffset ? OptionalLong.of(offset) : OptionalLong.empty();
  }
}
