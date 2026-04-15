package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link StoreException}, {@link StorageException}, and {@link CorruptionException}. */
class ExceptionHierarchyTest {

  @Nested
  class StoreExceptionTest {

    @Test
    void constructor_message_extendsRuntimeException() {
      StoreException ex = new StoreException("test");
      assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void constructor_message_preservesMessage() {
      StoreException ex = new StoreException("something broke");
      assertThat(ex.getMessage()).isEqualTo("something broke");
    }

    @Test
    void constructor_messageAndCause_preservesBoth() {
      IOException cause = new IOException("disk full");
      StoreException ex = new StoreException("write failed", cause);
      assertThat(ex.getMessage()).isEqualTo("write failed");
      assertThat(ex.getCause()).isSameAs(cause);
    }
  }

  @Nested
  class StorageExceptionTest {

    @Test
    void constructor_extendsStoreException() {
      StorageException ex = new StorageException("io error", new IOException("bad"));
      assertThat(ex).isInstanceOf(StoreException.class);
    }

    @Test
    void constructor_messageAndCause_preservesBoth() {
      IOException cause = new IOException("disk full");
      StorageException ex = new StorageException("write failed", cause);
      assertThat(ex.getMessage()).isEqualTo("write failed");
      assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void constructor_withPath_includesPathInMessage() {
      Path path = Path.of("/data/wal/000001.log");
      IOException cause = new IOException("permission denied");
      StorageException ex = new StorageException("cannot open WAL", path, cause);
      assertThat(ex.getMessage()).contains("cannot open WAL");
      assertThat(ex.getMessage()).contains("/data/wal/000001.log");
      assertThat(ex.getPath()).isPresent();
      assertThat(ex.getPath().get()).isEqualTo(path);
    }

    @Test
    void constructor_withoutPath_returnsEmptyOptional() {
      StorageException ex = new StorageException("generic error", new IOException("x"));
      assertThat(ex.getPath()).isEmpty();
    }

    @Test
    void isCatchableAsStoreException() {
      Throwable thrown =
          catchThrowable(
              () -> {
                throw new StorageException("test", new IOException("x"));
              });
      assertThat(thrown).isInstanceOf(StoreException.class);
    }
  }

  @Nested
  class CorruptionExceptionTest {

    @Test
    void constructor_extendsStoreException() {
      CorruptionException ex = new CorruptionException("bad CRC");
      assertThat(ex).isInstanceOf(StoreException.class);
    }

    @Test
    void constructor_messageOnly_preservesMessage() {
      CorruptionException ex = new CorruptionException("truncated block");
      assertThat(ex.getMessage()).isEqualTo("truncated block");
      assertThat(ex.getPath()).isEmpty();
      assertThat(ex.getOffset()).isEmpty();
    }

    @Test
    void constructor_withPathAndOffset_includesBothInMessage() {
      Path path = Path.of("/data/sst/000005.sst");
      CorruptionException ex = new CorruptionException("bad magic number", path, 4096);
      assertThat(ex.getMessage()).contains("bad magic number");
      assertThat(ex.getMessage()).contains("000005.sst");
      assertThat(ex.getMessage()).contains("4096");
      assertThat(ex.getPath()).contains(path);
      assertThat(ex.getOffset()).isPresent();
      assertThat(ex.getOffset().getAsLong()).isEqualTo(4096L);
    }

    @Test
    void isCatchableAsStoreException() {
      Throwable thrown =
          catchThrowable(
              () -> {
                throw new CorruptionException("bad data");
              });
      assertThat(thrown).isInstanceOf(StoreException.class);
    }
  }
}
