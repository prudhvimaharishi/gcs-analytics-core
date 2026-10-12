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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.RangeSplitter.Part;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

/**
 * Reads one byte range of an object as parallel part requests. Every part writes straight into its
 * own slice of a single buffer, so splitting never allocates or copies data buffers.
 */
final class SplitRangeReader {

  /** Reads object bytes {@code [offset, offset + window.remaining())} into {@code window}. */
  @FunctionalInterface
  interface PartReader {
    void read(long offset, ByteBuffer window) throws IOException;
  }

  private final ExecutorService executorService;
  private final PartReader partReader;
  private final int maxPartSize;

  SplitRangeReader(ExecutorService executorService, PartReader partReader, int maxPartSize) {
    checkArgument(maxPartSize > 0, "maxPartSize must be positive: %s", maxPartSize);
    this.executorService = checkNotNull(executorService, "executorService cannot be null");
    this.partReader = checkNotNull(partReader, "partReader cannot be null");
    this.maxPartSize = maxPartSize;
  }

  /**
   * Reads object bytes {@code [offset, offset + length)} into one buffer from {@code allocate},
   * which the first part to start calls once. The returned future completes with that buffer,
   * positioned at the data, after every part is read, or exceptionally with the first failure.
   */
  CompletableFuture<ByteBuffer> readAsync(
      long offset, int length, IntFunction<ByteBuffer> allocate) {
    checkArgument(length > 0, "length must be positive: %s", length);
    BackgroundRead read = new BackgroundRead(offset, length, allocate);
    for (Part part : read.parts) {
      try {
        var unused = executorService.submit(() -> read.readPart(part));
      } catch (RejectedExecutionException e) {
        read.result.completeExceptionally(e);
        break;
      }
    }
    return read.result;
  }

  /**
   * Reads object bytes {@code [offset, offset + dst.remaining())} into {@code dst} without moving
   * its position. The calling thread reads the first part, then takes back every part the executor
   * has not started, so a busy executor cannot stall the read. Returns only after no part is still
   * writing into {@code dst}.
   */
  void read(long offset, ByteBuffer dst) throws IOException {
    checkArgument(dst.hasRemaining(), "dst must have bytes remaining");
    BlockingRead read = new BlockingRead(offset, dst);
    read.submitPartsAfterFirst();
    read.readPartIfUnclaimed(0);
    read.takeBackUnstartedParts();
    read.awaitParts();
  }

  /** Returns a view of {@code length} bytes that starts {@code offset} bytes after the position. */
  private static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
    ByteBuffer view = buffer.duplicate();
    int start = view.position() + offset;
    view.position(start);
    view.limit(start + length);
    return view.slice();
  }

  /** A split read whose parts all run on the executor and fill a lazily allocated buffer. */
  private final class BackgroundRead {
    private final long offset;
    private final int length;
    private final IntFunction<ByteBuffer> allocate;
    private final ImmutableList<Part> parts;
    private final AtomicInteger unreadParts;
    private final CompletableFuture<ByteBuffer> result = new CompletableFuture<>();
    private ByteBuffer buffer;

    BackgroundRead(long offset, int length, IntFunction<ByteBuffer> allocate) {
      this.offset = offset;
      this.length = length;
      this.allocate = allocate;
      this.parts = RangeSplitter.split(length, maxPartSize);
      this.unreadParts = new AtomicInteger(parts.size());
    }

    void readPart(Part part) {
      if (result.isDone()) {
        // An earlier part failed, so this request would be wasted.
        return;
      }
      ByteBuffer target;
      try {
        target = getOrAllocateBuffer();
        partReader.read(
            offset + part.getOffset(), slice(target, part.getOffset(), part.getLength()));
      } catch (IOException | RuntimeException | Error e) {
        result.completeExceptionally(e);
        return;
      }
      if (unreadParts.decrementAndGet() == 0) {
        target.limit(target.position() + length);
        result.complete(target);
      }
    }

    private synchronized ByteBuffer getOrAllocateBuffer() {
      if (buffer == null) {
        ByteBuffer allocated = allocate.apply(length);
        checkArgument(allocated != null, "Buffer allocation returned null for %s bytes", length);
        checkArgument(
            allocated.remaining() >= length,
            "Allocated buffer has %s bytes remaining but %s are needed",
            allocated.remaining(),
            length);
        buffer = allocated;
      }
      return buffer;
    }
  }

  /** A split read into the caller's buffer, which the caller helps fill and waits for. */
  private final class BlockingRead {
    private final long offset;
    private final ByteBuffer dst;
    private final ImmutableList<Part> parts;
    private final AtomicIntegerArray claimedParts;
    private final CountDownLatch unfinishedParts;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    BlockingRead(long offset, ByteBuffer dst) {
      this.offset = offset;
      this.dst = dst;
      this.parts = RangeSplitter.split(dst.remaining(), maxPartSize);
      this.claimedParts = new AtomicIntegerArray(parts.size());
      this.unfinishedParts = new CountDownLatch(parts.size());
    }

    void submitPartsAfterFirst() {
      for (int index = 1; index < parts.size(); index++) {
        int partIndex = index;
        try {
          var unused = executorService.submit(() -> readPartIfUnclaimed(partIndex));
        } catch (RejectedExecutionException e) {
          // The caller takes back every part that was not submitted.
          return;
        }
      }
    }

    void readPartIfUnclaimed(int index) {
      if (claim(index)) {
        readParts(index, index + 1);
      }
    }

    /** Claims every part no executor thread has started and reads adjacent ones as one request. */
    void takeBackUnstartedParts() {
      int index = 0;
      while (index < parts.size()) {
        if (!claim(index)) {
          index++;
          continue;
        }
        int end = index + 1;
        while (end < parts.size() && claim(end)) {
          end++;
        }
        readParts(index, end);
        index = end;
      }
    }

    void awaitParts() throws IOException {
      Uninterruptibles.awaitUninterruptibly(unfinishedParts);
      Throwable firstFailure = failure.get();
      if (firstFailure instanceof IOException) {
        throw (IOException) firstFailure;
      }
      if (firstFailure instanceof RuntimeException) {
        throw (RuntimeException) firstFailure;
      }
      if (firstFailure != null) {
        throw (Error) firstFailure;
      }
    }

    private boolean claim(int index) {
      return claimedParts.compareAndSet(index, 0, 1);
    }

    /** Reads parts {@code [from, to)} as one request unless an earlier part already failed. */
    private void readParts(int from, int to) {
      try {
        if (failure.get() == null) {
          int start = parts.get(from).getOffset();
          Part last = parts.get(to - 1);
          int length = last.getOffset() + last.getLength() - start;
          partReader.read(offset + start, slice(dst, start, length));
        }
      } catch (IOException | RuntimeException | Error e) {
        failure.compareAndSet(null, e);
      } finally {
        for (int index = from; index < to; index++) {
          unfinishedParts.countDown();
        }
      }
    }
  }
}
