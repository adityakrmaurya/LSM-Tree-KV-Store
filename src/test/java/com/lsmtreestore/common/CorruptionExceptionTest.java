package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CorruptionException}.
 *
 * <p>Covers message-only and full constructors, inheritance, and the {@link
 * CorruptionException#toString()} format that surfaces the file path and offset for operator
 * diagnostics.
 */
class CorruptionExceptionTest {

  @Test
  void messageConstructor_setsMessageAndEmptyPathAndNegativeOneOffset() {
    CorruptionException ex = new CorruptionException("bad CRC");

    assertThat(ex.getMessage()).isEqualTo("bad CRC");
    assertThat(ex.path()).isEmpty();
    assertThat(ex.offset()).isEqualTo(-1L);
  }

  @Test
  void fullConstructor_setsAllFields() {
    Path file = Paths.get("/data/db/MANIFEST-0001");
    CorruptionException ex = new CorruptionException("magic mismatch", file, 4096L);

    assertThat(ex.getMessage()).isEqualTo("magic mismatch");
    assertThat(ex.path()).contains(file);
    assertThat(ex.offset()).isEqualTo(4096L);
  }

  @Test
  void fullConstructor_nullPath_returnsEmptyOptional() {
    CorruptionException ex = new CorruptionException("truncated", null, 0L);

    assertThat(ex.path()).isEmpty();
    assertThat(ex.offset()).isZero();
  }

  @Test
  void inheritsFromStoreException_andFromRuntimeException() {
    CorruptionException ex = new CorruptionException("bad");

    assertThat(ex).isInstanceOf(StoreException.class);
    assertThat(ex).isInstanceOf(RuntimeException.class);
  }

  @Test
  void toString_fullConstructor_includesPathAndOffset() {
    Path file = Paths.get("/data/db/000007.sst");
    CorruptionException ex = new CorruptionException("bad block CRC", file, 8192L);

    String rendered = ex.toString();
    assertThat(rendered).contains("bad block CRC");
    assertThat(rendered).contains("000007.sst");
    assertThat(rendered).contains("8192");
  }

  @Test
  void toString_messageOnly_doesNotIncludePathOrOffsetToken() {
    CorruptionException ex = new CorruptionException("bad varint");

    assertThat(ex.toString()).contains("bad varint");
    // No path was provided, so the rendered form must not fabricate one.
    assertThat(ex.toString()).doesNotContain("path=");
  }
}
