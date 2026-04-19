package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StoreException}, the base of the KV store exception hierarchy.
 *
 * <p>Verifies it is an unchecked {@link RuntimeException} and preserves both message-only and
 * message-with-cause construction.
 */
class StoreExceptionTest {

  @Test
  void messageConstructor_setsMessageAndNullCause() {
    StoreException ex = new StoreException("boom");

    assertThat(ex.getMessage()).isEqualTo("boom");
    assertThat(ex.getCause()).isNull();
  }

  @Test
  void messageAndCauseConstructor_setsBoth() {
    Throwable cause = new IllegalStateException("inner");
    StoreException ex = new StoreException("boom", cause);

    assertThat(ex.getMessage()).isEqualTo("boom");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void isUncheckedRuntimeException_byDesign() {
    // Storage engine errors are typically unrecoverable at the call site; using an unchecked base
    // matches RocksDB's RocksDBException and avoids polluting every signature with throws.
    assertThat(new StoreException("x")).isInstanceOf(RuntimeException.class);
  }
}
