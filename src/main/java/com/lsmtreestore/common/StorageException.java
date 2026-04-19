package com.lsmtreestore.common;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Thrown for I/O and filesystem failures — file-not-found, disk-full, permission-denied, and any
 * other {@link java.io.IOException}-class condition raised while reading or writing persistent
 * state.
 *
 * <p>Carries an optional {@link Path} pointing at the file that triggered the failure, which is
 * included in {@link #toString()} to aid operator diagnostics.
 */
public class StorageException extends StoreException {

  private static final long serialVersionUID = 1L;

  // Optional — null means "the caller did not know which file caused the failure".
  private final Path path;

  /**
   * Creates a new {@code StorageException} wrapping an underlying I/O cause with no associated file
   * path.
   *
   * @param message human-readable description of the failure
   * @param cause the underlying exception (typically an {@link java.io.IOException})
   */
  public StorageException(String message, Throwable cause) {
    super(message, cause);
    this.path = null;
  }

  /**
   * Creates a new {@code StorageException} for a failure localized to a specific file.
   *
   * @param message human-readable description of the failure
   * @param path file whose I/O triggered the failure; may be {@code null}
   * @param cause the underlying exception (typically an {@link java.io.IOException})
   */
  public StorageException(String message, Path path, Throwable cause) {
    super(message, cause);
    this.path = path;
  }

  /**
   * Returns the file whose I/O triggered this failure, if known.
   *
   * @return the file path, or {@link Optional#empty()} if the caller did not supply one
   */
  public Optional<Path> path() {
    return Optional.ofNullable(path);
  }

  @Override
  public String toString() {
    if (path == null) {
      return super.toString();
    }
    return super.toString() + " [path=" + path + "]";
  }
}
