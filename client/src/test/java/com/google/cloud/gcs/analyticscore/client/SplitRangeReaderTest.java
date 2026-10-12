/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.RangeSplitter.Part;
import com.google.cloud.gcs.analyticscore.client.SplitRangeReader.PartReader;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SplitRangeReaderTest {

  private static final byte[] OBJECT_DATA = TestDataGenerator.generateSeededRandomBytes(100, 42L);
  private static final int MAX_PART_SIZE = 8;
  private static final long RANGE_OFFSET = 10;
  private static final int RANGE_LENGTH = 20;

  private final ExecutorService executorService = Executors.newFixedThreadPool(4);
  private final ExecutorService blockedExecutorService = Executors.newSingleThreadExecutor();
  private final CountDownLatch unblockExecutor = new CountDownLatch(1);
  private final List<Part> requests = Collections.synchronizedList(new ArrayList<>());

  @AfterEach
  void tearDown() {
    unblockExecutor.countDown();
    executorService.shutdownNow();
    blockedExecutorService.shutdownNow();
  }

  @Test
  void constructor_nonPositiveMaxPartSize_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SplitRangeReader(executorService, recordingReader(), 0));

    assertThat(e).hasMessageThat().isEqualTo("maxPartSize must be positive: 0");
  }

  @Test
  void readAsync_fillsBufferWithRangeBytes() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    ByteBuffer buffer = reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, ByteBuffer::allocate).get();

    assertThat(remainingBytes(buffer)).isEqualTo(expectedRangeBytes());
  }

  @Test
  void readAsync_readsEachPartAsSeparateRequest() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, ByteBuffer::allocate).get();

    assertThat(requests)
        .containsExactly(Part.create(10, 7), Part.create(17, 7), Part.create(24, 6));
  }

  @Test
  void readAsync_allocatesOneBufferForAllParts() throws Exception {
    AtomicInteger allocations = new AtomicInteger();
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    reader
        .readAsync(
            RANGE_OFFSET,
            RANGE_LENGTH,
            size -> {
              allocations.incrementAndGet();
              return ByteBuffer.allocate(size);
            })
        .get();

    assertThat(allocations.get()).isEqualTo(1);
  }

  @Test
  void readAsync_largerBuffer_limitsBufferToRangeBytes() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    ByteBuffer buffer =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, size -> ByteBuffer.allocate(size + 5)).get();

    assertThat(buffer.remaining()).isEqualTo(RANGE_LENGTH);
  }

  @Test
  void readAsync_partFails_completesExceptionallyWithFailure() {
    IOException failure = new IOException("part failed");
    SplitRangeReader reader =
        new SplitRangeReader(executorService, failingReaderAt(17, failure), MAX_PART_SIZE);

    CompletableFuture<ByteBuffer> result =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, ByteBuffer::allocate);

    ExecutionException e = assertThrows(ExecutionException.class, result::get);
    assertThat(e).hasCauseThat().isSameInstanceAs(failure);
  }

  @Test
  void readAsync_firstPartFails_skipsPartsNotYetStarted() throws Exception {
    ExecutorService sequentialExecutor = Executors.newSingleThreadExecutor();
    SplitRangeReader reader =
        new SplitRangeReader(
            sequentialExecutor, failingReaderAt(10, new IOException("failed")), MAX_PART_SIZE);

    CompletableFuture<ByteBuffer> result =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, ByteBuffer::allocate);
    sequentialExecutor.shutdown();
    sequentialExecutor.awaitTermination(5, TimeUnit.SECONDS);

    assertThat(result.isCompletedExceptionally()).isTrue();
    assertThat(requests).containsExactly(Part.create(10, 7));
  }

  @Test
  void readAsync_allocationReturnsNull_completesExceptionally() {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    CompletableFuture<ByteBuffer> result =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, size -> null);

    ExecutionException e = assertThrows(ExecutionException.class, result::get);
    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .isEqualTo("Buffer allocation returned null for 20 bytes");
  }

  @Test
  void readAsync_allocatedBufferTooSmall_completesExceptionally() {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    CompletableFuture<ByteBuffer> result =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, size -> ByteBuffer.allocate(size - 1));

    ExecutionException e = assertThrows(ExecutionException.class, result::get);
    assertThat(e)
        .hasCauseThat()
        .hasMessageThat()
        .isEqualTo("Allocated buffer has 19 bytes remaining but 20 are needed");
  }

  @Test
  void readAsync_executorRejectsParts_completesExceptionally() {
    executorService.shutdown();
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    CompletableFuture<ByteBuffer> result =
        reader.readAsync(RANGE_OFFSET, RANGE_LENGTH, ByteBuffer::allocate);

    ExecutionException e = assertThrows(ExecutionException.class, result::get);
    assertThat(e).hasCauseThat().isInstanceOf(RejectedExecutionException.class);
  }

  @Test
  void readAsync_zeroLength_throwsIllegalArgumentException() {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> reader.readAsync(RANGE_OFFSET, 0, ByteBuffer::allocate));

    assertThat(e).hasMessageThat().isEqualTo("length must be positive: 0");
  }

  @Test
  void read_fillsCallerBufferWithRangeBytes() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);
    ByteBuffer dst = ByteBuffer.allocate(RANGE_LENGTH);

    reader.read(RANGE_OFFSET, dst);

    assertThat(dst.array()).isEqualTo(expectedRangeBytes());
  }

  @Test
  void read_doesNotMoveBufferPosition() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);
    ByteBuffer dst = ByteBuffer.allocate(RANGE_LENGTH + 3);
    dst.position(3);

    reader.read(RANGE_OFFSET, dst);

    assertThat(dst.position()).isEqualTo(3);
  }

  @Test
  void read_bufferWithOffset_writesRangeBytesAfterPosition() throws Exception {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);
    ByteBuffer dst = ByteBuffer.allocate(RANGE_LENGTH + 3);
    dst.position(3);

    reader.read(RANGE_OFFSET, dst);

    assertThat(remainingBytes(dst)).isEqualTo(expectedRangeBytes());
  }

  @Test
  void read_busyExecutor_takesBackUnstartedPartsAsOneRequest() throws Exception {
    blockExecutor();
    SplitRangeReader reader =
        new SplitRangeReader(blockedExecutorService, recordingReader(), MAX_PART_SIZE);

    reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH));

    assertThat(requests).containsExactly(Part.create(10, 7), Part.create(17, 13)).inOrder();
  }

  @Test
  void read_executorStartedMiddlePart_takesBackPartsOnEitherSideSeparately() throws Exception {
    CountDownLatch middlePartStarted = new CountDownLatch(1);
    PartReader recordingReader = recordingReader();
    PartReader partReader =
        (offset, window) -> {
          if (offset == 26) {
            middlePartStarted.countDown();
          }
          if (offset == 10) {
            Uninterruptibles.awaitUninterruptibly(middlePartStarted);
          }
          recordingReader.read(offset, window);
        };
    SplitRangeReader reader =
        new SplitRangeReader(new SingleSubmissionExecutor(1), partReader, MAX_PART_SIZE);

    reader.read(RANGE_OFFSET, ByteBuffer.allocate(4 * MAX_PART_SIZE));

    assertThat(requests)
        .containsExactly(
            Part.create(10, 8), Part.create(18, 8), Part.create(26, 8), Part.create(34, 8));
  }

  @Test
  void read_executorRejectsParts_readsAllPartsOnCallingThread() throws Exception {
    executorService.shutdown();
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);
    ByteBuffer dst = ByteBuffer.allocate(RANGE_LENGTH);

    reader.read(RANGE_OFFSET, dst);

    assertThat(dst.array()).isEqualTo(expectedRangeBytes());
  }

  @Test
  void read_partFails_throwsSameException() {
    IOException failure = new IOException("part failed");
    SplitRangeReader reader =
        new SplitRangeReader(executorService, failingReaderAt(10, failure), MAX_PART_SIZE);

    IOException thrown =
        assertThrows(
            IOException.class, () -> reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH)));

    assertThat(thrown).isSameInstanceAs(failure);
  }

  @Test
  void read_partThrowsRuntimeException_rethrowsSameException() {
    IllegalStateException failure = new IllegalStateException("part failed");
    SplitRangeReader reader =
        new SplitRangeReader(
            executorService,
            (offset, window) -> {
              throw failure;
            },
            MAX_PART_SIZE);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH)));

    assertThat(thrown).isSameInstanceAs(failure);
  }

  @Test
  void read_partThrowsError_rethrowsSameError() {
    Error failure = new Error("part failed");
    SplitRangeReader reader =
        new SplitRangeReader(
            executorService,
            (offset, window) -> {
              throw failure;
            },
            MAX_PART_SIZE);

    Error thrown =
        assertThrows(
            Error.class, () -> reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH)));

    assertThat(thrown).isSameInstanceAs(failure);
  }

  @Test
  void read_firstPartFails_skipsPartsTakenBack() {
    blockExecutor();
    SplitRangeReader reader =
        new SplitRangeReader(
            blockedExecutorService, failingReaderAt(10, new IOException("failed")), MAX_PART_SIZE);

    assertThrows(
        IOException.class, () -> reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH)));

    assertThat(requests).containsExactly(Part.create(10, 7));
  }

  @Test
  void read_partFails_waitsForRunningPartsBeforeThrowing() throws Exception {
    CountDownLatch runningPartStarted = new CountDownLatch(1);
    CountDownLatch firstPartFailed = new CountDownLatch(1);
    CountDownLatch finishRunningPart = new CountDownLatch(1);
    IOException failure = new IOException("first part failed");
    PartReader partReader =
        (offset, window) -> {
          if (offset == 10) {
            Uninterruptibles.awaitUninterruptibly(runningPartStarted);
            firstPartFailed.countDown();
            throw failure;
          }
          runningPartStarted.countDown();
          Uninterruptibles.awaitUninterruptibly(finishRunningPart);
        };
    SplitRangeReader reader = new SplitRangeReader(executorService, partReader, MAX_PART_SIZE);
    ExecutorService callerThread = Executors.newSingleThreadExecutor();

    CompletableFuture<Void> read =
        CompletableFuture.runAsync(
            () -> {
              try {
                reader.read(RANGE_OFFSET, ByteBuffer.allocate(RANGE_LENGTH));
              } catch (IOException e) {
                throw new IllegalStateException(e);
              }
            },
            callerThread);
    boolean firstPartFailedInTime = firstPartFailed.await(5, TimeUnit.SECONDS);
    boolean doneWhilePartRunning = read.isDone();
    finishRunningPart.countDown();
    callerThread.shutdown();

    assertThat(firstPartFailedInTime).isTrue();
    assertThat(doneWhilePartRunning).isFalse();
    ExecutionException e = assertThrows(ExecutionException.class, read::get);
    assertThat(e).hasCauseThat().hasCauseThat().isSameInstanceAs(failure);
  }

  @Test
  void read_emptyBuffer_throwsIllegalArgumentException() {
    SplitRangeReader reader =
        new SplitRangeReader(executorService, recordingReader(), MAX_PART_SIZE);

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> reader.read(RANGE_OFFSET, ByteBuffer.allocate(0)));

    assertThat(e).hasMessageThat().isEqualTo("dst must have bytes remaining");
  }

  private PartReader recordingReader() {
    return (offset, window) -> {
      requests.add(Part.create((int) offset, window.remaining()));
      window.put(OBJECT_DATA, (int) offset, window.remaining());
    };
  }

  private PartReader failingReaderAt(long failingOffset, IOException failure) {
    PartReader recordingReader = recordingReader();
    return (offset, window) -> {
      if (offset != failingOffset) {
        recordingReader.read(offset, window);
        return;
      }
      requests.add(Part.create((int) offset, window.remaining()));
      throw failure;
    };
  }

  /** Occupies the only thread of the blocked executor so that submitted parts never start. */
  private void blockExecutor() {
    var unused =
        blockedExecutorService.submit(() -> Uninterruptibles.awaitUninterruptibly(unblockExecutor));
  }

  private static byte[] expectedRangeBytes() {
    return Arrays.copyOfRange(OBJECT_DATA, (int) RANGE_OFFSET, (int) RANGE_OFFSET + RANGE_LENGTH);
  }

  private static byte[] remainingBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  /** Starts only one chosen submission, on its own thread, and never starts the others. */
  private static final class SingleSubmissionExecutor extends AbstractExecutorService {
    private final int submissionToStart;
    private int submissions;

    SingleSubmissionExecutor(int submissionToStart) {
      this.submissionToStart = submissionToStart;
    }

    @Override
    public void execute(Runnable command) {
      if (submissions++ == submissionToStart) {
        new Thread(command).start();
      }
    }

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      return ImmutableList.of();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
  }
}
