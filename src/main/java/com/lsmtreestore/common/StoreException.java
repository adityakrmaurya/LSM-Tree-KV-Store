package com.lsmtreestore.common;

/**
 * Base exception for all LSM Tree KV Store errors.
 *
 * <p>Extends {@link RuntimeException} (unchecked) because storage engine errors are typically
 * unrecoverable at the point they occur. Checked exceptions would force try/catch at every call
 * site without adding safety. Callers who want to handle errors can still catch {@code
 * StoreException} as a base type.
 */
public class StoreException extends RuntimeException {

  /**
   * Creates a store exception with the given message.
   *
   * @param message description of the error
   */
  public StoreException(String message) {
    super(message);
  }

  /**
   * Creates a store exception with the given message and cause.
   *
   * @param message description of the error
   * @param cause the underlying cause
   */
  public StoreException(String message, Throwable cause) {
    super(message, cause);
  }
}
