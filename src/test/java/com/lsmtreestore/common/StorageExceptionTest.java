package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StorageException}.
 *
 * <p>Covers I/O-cause wrapping, optional path carrying, inheritance from {@link StoreException},
 * and the {@link StorageException#toString()} format that includes the path when known.
 */
class StorageExceptionTest {

  @Test
  void messageAndCauseConstructor_setsFieldsAndNullPath() {
    IOException cause = new IOException("disk full");
    StorageException ex = new StorageException("write failed", cause);

    assertThat(ex.getMessage()).isEqualTo("write failed");
    assertThat(ex.getCause()).isSameAs(cause);
    assertThat(ex.path()).isEmpty();
  }

  @Test
  void messagePathCauseConstructor_setsAllThree() {
    Path path = Paths.get("/tmp/db/000001.log");
    IOException cause = new IOException("disk full");
    StorageException ex = new StorageException("write failed", path, cause);

    assertThat(ex.getMessage()).isEqualTo("write failed");
    assertThat(ex.path()).contains(path);
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void messagePathCauseConstructor_nullPath_returnsEmptyOptional() {
    StorageException ex = new StorageException("write failed", null, new IOException());

    assertThat(ex.path()).isEmpty();
  }

  @Test
  void inheritsFromStoreException_andFromRuntimeException() {
    StorageException ex = new StorageException("x", new IOException());

    assertThat(ex).isInstanceOf(StoreException.class);
    assertThat(ex).isInstanceOf(RuntimeException.class);
  }

  @Test
  void toString_withoutPath_containsMessage() {
    StorageException ex = new StorageException("boom", new IOException());

    assertThat(ex.toString()).contains("boom");
  }

  @Test
  void toString_withPath_includesPath() {
    Path path = Paths.get("/var/lsm/000042.sst");
    StorageException ex = new StorageException("read failed", path, new IOException());

    assertThat(ex.toString()).contains("read failed").contains("000042.sst");
  }
}
