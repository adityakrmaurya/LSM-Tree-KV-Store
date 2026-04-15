package com.lsmtreestore.common;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Exception for I/O and file system failures.
 *
 * <p>Thrown when the storage engine encounters file-level errors such as file not found, disk full,
 * or permission denied. Optionally includes the {@link Path} of the file that caused the error for
 * diagnostic logging.
 */
public class StorageException extends StoreException {

  private final Path path;

  /**
   * Creates a storage exception with the given message and cause.
   *
   * @param message description of the error
   * @param cause the underlying I/O cause
   */
  public StorageException(String message, Throwable cause) {
    super(message, cause);
    this.path = null;
  }

  /**
   * Creates a storage exception with a file path and cause.
   *
   * @param message description of the error
   * @param path the file that caused the error
   * @param cause the underlying I/O cause
   */
  public StorageException(String message, Path path, Throwable cause) {
    super(message + " [path=" + path + "]", cause);
    this.path = path;
  }

  /**
   * Returns the file path associated with this error, if available.
   *
   * @return an {@link Optional} containing the path, or empty if not applicable
   */
  public Optional<Path> getPath() {
    return Optional.ofNullable(path);
  }
}
