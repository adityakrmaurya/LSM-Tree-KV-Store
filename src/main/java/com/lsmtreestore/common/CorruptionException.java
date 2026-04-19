package com.lsmtreestore.common;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Thrown for data-integrity violations detected while reading persistent state — bad CRC, truncated
 * file, invalid magic number, malformed varint, or any other shape-violation of the on-disk format.
 *
 * <p>Carries an optional {@link Path} and byte offset pointing at where the corruption was
 * detected, which is included in {@link #toString()} to support forensic analysis.
 */
public class CorruptionException extends StoreException {

  private static final long serialVersionUID = 1L;

  // Sentinel used when no offset is known — byte offsets in this codebase are always non-negative.
  private static final long UNKNOWN_OFFSET = -1L;

  private final Path path;
  private final long offset;

  /**
   * Creates a new {@code CorruptionException} with only a message (no file or offset known).
   *
   * @param message human-readable description of the corruption
   */
  public CorruptionException(String message) {
    super(message);
    this.path = null;
    this.offset = UNKNOWN_OFFSET;
  }

  /**
   * Creates a new {@code CorruptionException} pointing at the byte offset in a specific file where
   * the corruption was detected.
   *
   * @param message human-readable description of the corruption
   * @param path file where the corruption was detected; may be {@code null}
   * @param offset byte offset into {@code path} where the corruption was detected
   */
  public CorruptionException(String message, Path path, long offset) {
    super(message);
    this.path = path;
    this.offset = offset;
  }

  /**
   * Returns the file where corruption was detected, if known.
   *
   * @return the file path, or {@link Optional#empty()} if the caller did not supply one
   */
  public Optional<Path> path() {
    return Optional.ofNullable(path);
  }

  /**
   * Returns the byte offset at which corruption was detected, or {@code -1} if unknown.
   *
   * @return non-negative offset when known, otherwise {@code -1}
   */
  public long offset() {
    return offset;
  }

  @Override
  public String toString() {
    if (path == null && offset == UNKNOWN_OFFSET) {
      return super.toString();
    }
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" [");
    boolean first = true;
    if (path != null) {
      sb.append("path=").append(path);
      first = false;
    }
    if (offset != UNKNOWN_OFFSET) {
      if (!first) {
        sb.append(", ");
      }
      sb.append("offset=").append(offset);
    }
    sb.append("]");
    return sb.toString();
  }
}
