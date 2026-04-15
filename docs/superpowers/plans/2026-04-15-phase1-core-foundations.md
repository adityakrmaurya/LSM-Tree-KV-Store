# Phase 1: Core Foundations — Remaining Issues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the Phase 1 epic (#2) by implementing the three remaining user stories: custom exception hierarchy (#11), CRC32C checksum utility (#10), and DBConfig record (#12).

**Architecture:** Bottom-up build order — exceptions first (zero deps, used by Checksum and DBConfig), then Checksum (uses exceptions for corruption errors), then DBConfig (uses exceptions for validation). All classes follow Google Java Style, Java 21 conventions (records, sealed interfaces where appropriate), and target 95%+ test coverage via TDD.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Gradle 9.4.1, Spotless (Google Java Format), Checkstyle

---

## File Structure

### New files to create:

| File | Responsibility |
|------|---------------|
| `src/main/java/com/lsmtreestore/common/StoreException.java` | Base unchecked exception for all KV store errors |
| `src/main/java/com/lsmtreestore/common/StorageException.java` | I/O and file system failure exception |
| `src/main/java/com/lsmtreestore/common/CorruptionException.java` | Data integrity violation exception |
| `src/test/java/com/lsmtreestore/common/ExceptionHierarchyTest.java` | Tests for all three exceptions |
| `src/main/java/com/lsmtreestore/common/Checksum.java` | CRC32C checksum compute/verify/mask/unmask |
| `src/test/java/com/lsmtreestore/common/ChecksumTest.java` | Tests for Checksum utility |
| `src/main/java/com/lsmtreestore/config/DBConfig.java` | Configuration record with builder and validation |
| `src/test/java/com/lsmtreestore/config/DBConfigTest.java` | Tests for DBConfig record and builder |

### Existing files (no modifications needed):
- `src/main/java/com/lsmtreestore/common/Bytes.java` -- complete
- `src/main/java/com/lsmtreestore/common/Coding.java` -- complete
- `src/test/java/com/lsmtreestore/common/BytesTest.java` -- complete
- `src/test/java/com/lsmtreestore/common/CodingTest.java` -- complete

---

## Task 1: Exception Hierarchy (Issue #11)

**Files:**
- Create: `src/main/java/com/lsmtreestore/common/StoreException.java`
- Create: `src/main/java/com/lsmtreestore/common/StorageException.java`
- Create: `src/main/java/com/lsmtreestore/common/CorruptionException.java`
- Create: `src/test/java/com/lsmtreestore/common/ExceptionHierarchyTest.java`

### Task 1.1: Write failing tests for StoreException

- [ ] **Step 1: Write the test file with StoreException tests**

```java
package com.lsmtreestore.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExceptionHierarchyTest {

  @Nested
  class StoreExceptionTests {

    @Test
    void storeException_extendsRuntimeException() {
      StoreException ex = new StoreException("test");
      assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void storeException_messageConstructor_preservesMessage() {
      StoreException ex = new StoreException("something broke");
      assertThat(ex.getMessage()).isEqualTo("something broke");
    }

    @Test
    void storeException_messageAndCauseConstructor_preservesBoth() {
      IOException cause = new IOException("disk full");
      StoreException ex = new StoreException("write failed", cause);
      assertThat(ex.getMessage()).isEqualTo("write failed");
      assertThat(ex.getCause()).isSameAs(cause);
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "com.lsmtreestore.common.ExceptionHierarchyTest" 2>&1`
Expected: Compilation failure — `StoreException` class not found.

### Task 1.2: Implement StoreException

- [ ] **Step 3: Write StoreException implementation**

```java
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
```

- [ ] **Step 4: Run tests to verify StoreException tests pass**

Run: `./gradlew test --tests "com.lsmtreestore.common.ExceptionHierarchyTest.StoreExceptionTests" 2>&1`
Expected: All 3 tests PASS.

### Task 1.3: Write failing tests and implement StorageException

- [ ] **Step 5: Add StorageException tests to ExceptionHierarchyTest**

Add this nested class:

```java
@Nested
class StorageExceptionTests {

  @Test
  void storageException_extendsStoreException() {
    StorageException ex = new StorageException("io error", new IOException("bad"));
    assertThat(ex).isInstanceOf(StoreException.class);
  }

  @Test
  void storageException_messageAndCause_preservesBoth() {
    IOException cause = new IOException("disk full");
    StorageException ex = new StorageException("write failed", cause);
    assertThat(ex.getMessage()).isEqualTo("write failed");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void storageException_withPath_includesPathInMessage() {
    Path path = Path.of("/data/wal/000001.log");
    IOException cause = new IOException("permission denied");
    StorageException ex = new StorageException("cannot open WAL", path, cause);
    assertThat(ex.getMessage()).contains("cannot open WAL");
    assertThat(ex.getMessage()).contains("/data/wal/000001.log");
    assertThat(ex.getPath()).isPresent();
    assertThat(ex.getPath().get()).isEqualTo(path);
  }

  @Test
  void storageException_withoutPath_returnsEmptyOptional() {
    StorageException ex = new StorageException("generic error", new IOException("x"));
    assertThat(ex.getPath()).isEmpty();
  }

  @Test
  void storageException_isCatchableAsStoreException() {
    Throwable thrown = catchThrowable(() -> {
      throw new StorageException("test", new IOException("x"));
    });
    assertThat(thrown).isInstanceOf(StoreException.class);
  }
}
```

- [ ] **Step 6: Run test — expect compilation failure (StorageException missing)**

- [ ] **Step 7: Implement StorageException**

```java
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
```

- [ ] **Step 8: Run tests — expect all StorageException tests pass**

### Task 1.4: Write failing tests and implement CorruptionException

- [ ] **Step 9: Add CorruptionException tests**

```java
@Nested
class CorruptionExceptionTests {

  @Test
  void corruptionException_extendsStoreException() {
    CorruptionException ex = new CorruptionException("bad CRC");
    assertThat(ex).isInstanceOf(StoreException.class);
  }

  @Test
  void corruptionException_messageOnly_preservesMessage() {
    CorruptionException ex = new CorruptionException("truncated block");
    assertThat(ex.getMessage()).isEqualTo("truncated block");
    assertThat(ex.getPath()).isEmpty();
    assertThat(ex.getOffset()).isEmpty();
  }

  @Test
  void corruptionException_withPathAndOffset_includesBothInMessage() {
    Path path = Path.of("/data/sst/000005.sst");
    CorruptionException ex = new CorruptionException("bad magic number", path, 4096);
    assertThat(ex.getMessage()).contains("bad magic number");
    assertThat(ex.getMessage()).contains("000005.sst");
    assertThat(ex.getMessage()).contains("4096");
    assertThat(ex.getPath()).contains(path);
    assertThat(ex.getOffset()).contains(4096L);
  }

  @Test
  void corruptionException_isCatchableAsStoreException() {
    Throwable thrown = catchThrowable(() -> {
      throw new CorruptionException("bad data");
    });
    assertThat(thrown).isInstanceOf(StoreException.class);
  }
}
```

- [ ] **Step 10: Run test — expect compilation failure**

- [ ] **Step 11: Implement CorruptionException**

```java
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
```

- [ ] **Step 12: Run all exception tests — expect ALL pass**

Run: `./gradlew test --tests "com.lsmtreestore.common.ExceptionHierarchyTest" 2>&1`
Expected: All tests PASS.

- [ ] **Step 13: Run spotless and checkstyle**

Run: `./gradlew spotlessApply checkstyleMain checkstyleTest 2>&1`

- [ ] **Step 14: Commit**

```bash
git add src/main/java/com/lsmtreestore/common/StoreException.java \
        src/main/java/com/lsmtreestore/common/StorageException.java \
        src/main/java/com/lsmtreestore/common/CorruptionException.java \
        src/test/java/com/lsmtreestore/common/ExceptionHierarchyTest.java
git commit -m "feat: implement custom exception hierarchy (closes #11)"
```

---

## Task 2: CRC32C Checksum Utility (Issue #10)

**Files:**
- Create: `src/main/java/com/lsmtreestore/common/Checksum.java`
- Create: `src/test/java/com/lsmtreestore/common/ChecksumTest.java`

### Task 2.1: Write failing tests for Checksum

- [ ] **Step 1: Write ChecksumTest with compute, verify, mask/unmask tests**

Tests should cover:
- `compute(byte[])` returns correct CRC32C for known inputs
- `compute(byte[], offset, length)` works on sub-arrays
- `compute(emptyArray)` returns a valid (non-zero for CRC32C) checksum
- `verify(data, correctChecksum)` returns true
- `verify(data, wrongChecksum)` returns false
- `verify(corruptedData, originalChecksum)` returns false
- `mask(unmask(crc)) == crc` round-trip
- `unmask(mask(crc)) == crc` round-trip
- Masked value differs from original (prevents accidental collision)

- [ ] **Step 2: Run tests — expect compilation failure**

### Task 2.2: Implement Checksum

- [ ] **Step 3: Implement Checksum.java using java.util.zip.CRC32C**

Key design:
- Use `java.util.zip.CRC32C` (hardware-accelerated since Java 9)
- Mask formula: `((crc >>> 15) | (crc << 17)) + 0xa282ead8` (LevelDB convention)
- All static methods, private constructor
- Full Javadoc

- [ ] **Step 4: Run tests — expect all PASS**
- [ ] **Step 5: Run spotlessApply + checkstyle**
- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/lsmtreestore/common/Checksum.java \
        src/test/java/com/lsmtreestore/common/ChecksumTest.java
git commit -m "feat: implement CRC32C checksum utility (closes #10)"
```

---

## Task 3: DBConfig Record (Issue #12)

**Files:**
- Create: `src/main/java/com/lsmtreestore/config/DBConfig.java`
- Create: `src/test/java/com/lsmtreestore/config/DBConfigTest.java`

### Task 3.1: Write failing tests for DBConfig defaults

- [ ] **Step 1: Write tests for defaultConfig factory and all default values**

Tests should verify:
- `DBConfig.defaultConfig(path)` returns config with all 15 documented defaults
- `dbPath` matches provided path
- Each field matches documented default (4MB memtable, 2 immutable, etc.)

- [ ] **Step 2: Run tests — expect compilation failure**

### Task 3.2: Implement DBConfig record with defaults

- [ ] **Step 3: Implement DBConfig record with defaultConfig factory**
- [ ] **Step 4: Run tests — expect PASS**

### Task 3.3: Write failing tests for Builder

- [ ] **Step 5: Write tests for Builder overrides**

Tests should verify:
- Builder allows overriding any single field
- Builder preserves defaults for non-overridden fields
- Builder produces immutable record

- [ ] **Step 6: Run tests — expect failure (Builder not yet implemented)**

### Task 3.4: Implement Builder

- [ ] **Step 7: Implement static Builder inner class**
- [ ] **Step 8: Run tests — expect PASS**

### Task 3.5: Write failing tests for validation

- [ ] **Step 9: Write validation tests**

Tests should verify:
- Null dbPath throws `NullPointerException`
- `maxMemTableSize <= 0` throws `IllegalArgumentException`
- `blockSize <= 0` throws `IllegalArgumentException`
- `blockSize` not power of 2 throws `IllegalArgumentException`
- `maxLevels < 2` throws `IllegalArgumentException`
- `l0CompactionTrigger >= l0SlowdownTrigger` throws `IllegalArgumentException`
- `l0SlowdownTrigger >= l0StopTrigger` throws `IllegalArgumentException`
- `bloomFilterBitsPerKey < 1` throws `IllegalArgumentException`
- Valid edge case values (minimum valid) succeed

- [ ] **Step 10: Run tests — expect failures (validation not yet implemented)**

### Task 3.6: Implement validation

- [ ] **Step 11: Add compact constructor with all validation logic**
- [ ] **Step 12: Run tests — expect PASS**

### Task 3.7: Write failing tests for helper methods

- [ ] **Step 13: Write tests for maxBytesForLevel and targetFileSize**

Tests should verify:
- `maxBytesForLevel(1)` = l1MaxBytes
- `maxBytesForLevel(2)` = l1MaxBytes * levelSizeMultiplier
- `maxBytesForLevel(n)` = l1MaxBytes * levelSizeMultiplier^(n-1)
- `targetFileSize` scales appropriately

- [ ] **Step 14: Run tests — expect failure**

### Task 3.8: Implement helper methods

- [ ] **Step 15: Implement maxBytesForLevel and targetFileSize**
- [ ] **Step 16: Run tests — expect PASS**
- [ ] **Step 17: Run spotlessApply + checkstyle**
- [ ] **Step 18: Commit**

```bash
git add src/main/java/com/lsmtreestore/config/DBConfig.java \
        src/test/java/com/lsmtreestore/config/DBConfigTest.java
git commit -m "feat: implement DBConfig record with builder and validation (closes #12)"
```

---

## Task 4: Final Verification

- [ ] **Step 1: Run full build**

Run: `./gradlew build --no-daemon 2>&1`
Expected: BUILD SUCCESSFUL (compile + test + spotless + checkstyle)

- [ ] **Step 2: Generate coverage report and verify 90%+**

Run: `./gradlew jacocoTestReport --no-daemon 2>&1`

- [ ] **Step 3: Security review of all new code**

Review for: input validation, exception information leakage, path traversal in StorageException/CorruptionException paths.

- [ ] **Step 4: Push branch**

```bash
git push -u origin feature/phase1-core-foundations
```
