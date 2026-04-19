package com.lsmtreestore.common;

/**
 * Base exception for all LSM Tree KV store errors.
 *
 * <p>Extends {@link RuntimeException} because storage-engine failures are typically unrecoverable
 * at the point they occur — forcing {@code throws} on every call site would clutter the API without
 * improving safety. Callers that wish to handle errors explicitly can still catch {@code
 * StoreException} as a common base.
 *
 * <p>Concrete subclasses:
 *
 * <ul>
 *   <li>{@link StorageException} — I/O and filesystem failures
 *   <li>{@link CorruptionException} — data-integrity violations
 * </ul>
 */
public class StoreException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new {@code StoreException} with a message and no cause.
   *
   * @param message human-readable description of the failure
   */
  public StoreException(String message) {
    super(message);
  }

  /**
   * Creates a new {@code StoreException} wrapping an underlying cause.
   *
   * @param message human-readable description of the failure
   * @param cause the underlying exception that triggered this failure; may be {@code null}
   */
  public StoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
