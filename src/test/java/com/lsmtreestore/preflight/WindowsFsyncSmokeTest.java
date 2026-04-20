package com.lsmtreestore.preflight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-flight diagnostic for the WAL module's group-commit coordinator.
 *
 * <p>Models the coordinator's expected access pattern: a single {@link FileChannel} owned by one
 * WAL writer, with many producer threads contending for one serialization lock and whichever thread
 * holds the lock performs the {@link FileChannel#write(java.nio.ByteBuffer)} + {@link
 * FileChannel#force(boolean) force(false)}. This is step 0 of the PR #1 build order: before
 * investing a weekend on a coordinator whose marquee test depends on {@code force(false)} behaving
 * correctly under this pattern, prove the OS can actually handle it.
 *
 * <p><strong>What this test measures:</strong> high contention on a single writer slot. 100 virtual
 * threads repeatedly race for the lock; the winner writes 10 KB and {@code force}s durably. {@code
 * force(false)} is therefore called frequently but never concurrently on the same channel. This
 * matches the WAL's single-writer-per-file discipline and the leader-follower group commit design
 * (one "leader" does the I/O for everyone).
 *
 * <p><strong>What this test does NOT measure:</strong> concurrent {@code force(false)} calls from
 * multiple threads on the same channel. That access pattern is not used by the WAL coordinator. If
 * the coordinator design ever changes to allow concurrent {@code force} calls, this preflight would
 * need a second variant.
 *
 * <p>Disabled by default. To run:
 *
 * <pre>
 *   ./gradlew test --tests "com.lsmtreestore.preflight.WindowsFsyncSmokeTest" \
 *                  -Dpreflight.run=true [-Dpreflight.durationSec=60]
 * </pre>
 *
 * <p>Runs for a configurable duration (default 60 seconds). At the end asserts that no thread
 * threw, the file size on disk matches the sum of reported writes, and no torn write is visible at
 * the length level.
 *
 * <p>Pass criteria &rarr; platform handles the coordinator's access pattern; the marquee test can
 * run on that platform's CI. Fail criteria &rarr; either gate the marquee test to a known-good
 * platform, or switch the I/O strategy to {@code RandomAccessFile} + {@code FileDescriptor.sync()}.
 *
 * <p>This test is intentionally a diagnostic (not a unit test); it writes hundreds of MB to
 * {@code @TempDir} and runs for a full minute by default. The {@code @TempDir} infrastructure
 * cleans up the file after the test.
 */
@EnabledIfSystemProperty(named = "preflight.run", matches = "true")
class WindowsFsyncSmokeTest {

  private static final Logger LOG = LoggerFactory.getLogger(WindowsFsyncSmokeTest.class);

  private static final int WRITER_COUNT = 100;
  private static final int PAYLOAD_SIZE = 10 * 1024;
  private static final long DEFAULT_DURATION_SEC = 60L;
  private static final long JOIN_SLACK_SEC = 30L;

  /**
   * Smoke test: many virtual threads, tight append + force loop, for a minute.
   *
   * @param dir per-test temp directory provided by JUnit Jupiter
   * @throws Exception if the test harness itself fails (file creation, interrupt, etc.); errors
   *     from worker threads are captured via {@link AtomicReference} and surfaced as assertion
   *     failures rather than being rethrown here
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void force_under100VirtualThreadsAppendingFor60Sec_noDeadlockNoDataLoss(@TempDir Path dir)
      throws Exception {
    long durationSec = Long.getLong("preflight.durationSec", DEFAULT_DURATION_SEC);
    Path file = dir.resolve("preflight.log");

    // The payload array is shared across all worker threads. This is safe because it is filled
    // once above and never mutated afterwards; each worker wraps it in its own ByteBuffer (so
    // position/limit are thread-local) and reads only. Cloning per thread would cost 1 MB for no
    // behavioral benefit.
    byte[] payloadBytes = new byte[PAYLOAD_SIZE];
    Arrays.fill(payloadBytes, (byte) 0x5A);

    AtomicLong totalWrites = new AtomicLong(0);
    AtomicReference<Throwable> firstError = new AtomicReference<>();

    LOG.info(
        "preflight start: writers={}, payloadBytes={}, durationSec={}, file={}",
        WRITER_COUNT,
        PAYLOAD_SIZE,
        durationSec,
        file);

    try (FileChannel channel =
        FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {
      ReentrantLock lock = new ReentrantLock();
      CountDownLatch start = new CountDownLatch(1);
      long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSec);
      List<Thread> threads = new ArrayList<>(WRITER_COUNT);

      for (int i = 0; i < WRITER_COUNT; i++) {
        Thread t =
            Thread.ofVirtual()
                .name("preflight-writer-" + i)
                .unstarted(
                    () -> {
                      ByteBuffer buf = ByteBuffer.wrap(payloadBytes);
                      try {
                        start.await();
                        while (System.nanoTime() < deadlineNanos && firstError.get() == null) {
                          buf.clear();
                          lock.lock();
                          try {
                            while (buf.hasRemaining()) {
                              channel.write(buf);
                            }
                            channel.force(false);
                          } finally {
                            lock.unlock();
                          }
                          totalWrites.incrementAndGet();
                        }
                      } catch (Throwable err) {
                        firstError.compareAndSet(null, err);
                      }
                    });
        threads.add(t);
        t.start();
      }

      start.countDown();

      long joinDeadlineMs =
          System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(durationSec + JOIN_SLACK_SEC);
      for (Thread t : threads) {
        long remainingMs = joinDeadlineMs - System.currentTimeMillis();
        if (remainingMs <= 0) {
          fail(
              "Timed out waiting for worker threads to finish — probable deadlock\n"
                  + dump(threads));
        }
        t.join(remainingMs);
        if (t.isAlive()) {
          fail(
              "Worker thread " + t.getName() + " still alive after join timeout\n" + dump(threads));
        }
      }

      long finalWrites = totalWrites.get();
      long onDiskBytes = Files.size(file);
      long expectedBytes = finalWrites * (long) PAYLOAD_SIZE;

      LOG.info(
          "preflight end: writes={}, onDiskBytes={}, expectedBytes={}, throughputPerSec={}",
          finalWrites,
          onDiskBytes,
          expectedBytes,
          finalWrites / Math.max(durationSec, 1L));

      assertThat(firstError.get()).as("no worker thread threw an exception").isNull();
      assertThat(onDiskBytes)
          .as("file size on disk matches total bytes reported by workers")
          .isEqualTo(expectedBytes);
      assertThat(onDiskBytes % PAYLOAD_SIZE)
          .as("file size is an exact multiple of payload size — no torn writes at length level")
          .isZero();
      assertThat(finalWrites).as("at least one write actually happened").isPositive();
    }

    long reopenedSize = Files.size(file);
    LOG.info("preflight reopened size after channel close: {}", reopenedSize);
    assertThat(reopenedSize).as("file size stable after close").isPositive();
  }

  /**
   * Captures a thread-stack snapshot of any worker still alive. Used when the join loop fails so
   * that "probable deadlock" messages are actionable instead of opaque.
   *
   * @param threads the worker threads to inspect
   * @return a multi-line string listing each alive thread's name followed by its stack trace
   */
  private static String dump(List<Thread> threads) {
    StringBuilder sb = new StringBuilder();
    for (Thread t : threads) {
      if (t.isAlive()) {
        sb.append(t.getName()).append(":\n");
        for (StackTraceElement frame : t.getStackTrace()) {
          sb.append("  ").append(frame).append('\n');
        }
      }
    }
    return sb.isEmpty() ? "(no alive threads captured)" : sb.toString();
  }
}
