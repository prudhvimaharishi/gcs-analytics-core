/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * Schedules and cancels speculative range requests for a single open stream against {@link
 * PrefetchBufferCache}.
 *
 * <p>Ranges already present or in-flight in {@link PrefetchBufferCache} are skipped automatically.
 * Ranges larger than {@code maxBlockSizeBytes} are registered as a single contiguous entry in
 * {@link PrefetchBufferCache} while their network downloads are sliced into parallel {@code
 * maxBlockSizeBytes} requests writing directly into slices of one shared {@link ByteBuffer}.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  private static final Comparator<Range<Long>> BY_START_THEN_END =
      Comparator.<Range<Long>>comparingLong(Range::lowerEndpoint)
          .thenComparingLong(Range::upperEndpoint);

  private final GcsItemId itemId;
  private final PrefetchBufferCache bufferCache;
  private final long maxBlockSizeBytes;
  private final PrefetchBufferCache.RegisteredStream registeredStream;
  private final ConcurrentSkipListMap<Range<Long>, GcsObjectRange> inFlightRequests =
      new ConcurrentSkipListMap<>(BY_START_THEN_END);
  private final ConcurrentSkipListMap<Range<Long>, CompletableFuture<ByteBuffer>> registeredRanges =
      new ConcurrentSkipListMap<>(BY_START_THEN_END);

  PrefetchScheduler(GcsItemId itemId, PrefetchBufferCache bufferCache) {
    this(itemId, bufferCache, GcsPrefetchOptions.builder().build().getBlockSizeBytes());
  }

  PrefetchScheduler(GcsItemId itemId, PrefetchBufferCache bufferCache, long maxBlockSizeBytes) {
    checkArgument(
        maxBlockSizeBytes > 0, "maxBlockSizeBytes %s must be positive", maxBlockSizeBytes);
    this.itemId = checkNotNull(itemId, "itemId cannot be null");
    this.bufferCache = checkNotNull(bufferCache, "bufferCache cannot be null");
    this.maxBlockSizeBytes = maxBlockSizeBytes;
    this.registeredStream = this.bufferCache.registerStream(this.itemId);
  }

  /**
   * Schedules speculative reads for the given byte ranges, ignoring any that are already covered in
   * the cache.
   *
   * @param byteRanges exact closed-open byte ranges {@code [startOffset, endOffset)} in ascending
   *     order
   * @param fileSize the size of the object, used to truncate ranges extending past EOF
   * @return {@code true} if every range is now cached or in flight
   */
  boolean schedule(
      VectoredSeekableByteChannel source, Collection<Range<Long>> byteRanges, long fileSize) {
    List<GcsObjectRange> singleBlockRanges = new ArrayList<>();
    boolean allScheduled = true;
    for (Range<Long> byteRange : byteRanges) {
      long startOffset = byteRange.lowerEndpoint();
      long endOffset = Math.min(byteRange.upperEndpoint(), fileSize);
      int length = (int) (endOffset - startOffset);
      if (length <= 0) {
        continue;
      }
      if (length <= maxBlockSizeBytes) {
        tryRegisterSingleRange(startOffset, length).ifPresent(singleBlockRanges::add);
      } else if (!scheduleSlicedRange(source, startOffset, length)) {
        allScheduled = false;
      }
    }

    if (!singleBlockRanges.isEmpty()
        && !dispatchVectoredRead(source, singleBlockRanges, ByteBuffer::allocate)) {
      allScheduled = false;
    }
    return allScheduled;
  }

  private Optional<GcsObjectRange> tryRegisterSingleRange(long startOffset, int length) {
    CompletableFuture<ByteBuffer> fetchFuture = new CompletableFuture<>();
    GcsObjectRange objectRange = createObjectRange(startOffset, length, fetchFuture);
    Range<Long> key = rangeKeyOf(objectRange);
    if (!bufferCache.registerRange(
        itemId,
        startOffset,
        length,
        fetchFuture,
        () -> {
          inFlightRequests.remove(key, objectRange);
          objectRange.promote();
        })) {
      return Optional.empty();
    }
    trackInFlightSlice(objectRange);
    registeredRanges.put(key, fetchFuture);
    return Optional.of(objectRange);
  }

  private boolean scheduleSlicedRange(
      VectoredSeekableByteChannel source, long startOffset, int length) {
    CompletableFuture<ByteBuffer> fetchFuture = new CompletableFuture<>();
    ImmutableList<GcsObjectRange> slices = createSlices(startOffset, length);
    Range<Long> fullKey = Range.closedOpen(startOffset, startOffset + length);
    if (!bufferCache.registerRange(
        itemId, startOffset, length, fetchFuture, () -> promoteSlices(slices))) {
      return true;
    }
    registeredRanges.put(fullKey, fetchFuture);
    SlicedRangeBuffer sharedBuffer =
        new SlicedRangeBuffer(startOffset, length, slices, fetchFuture);
    for (GcsObjectRange slice : slices) {
      trackInFlightSlice(slice);
      CompletableFuture<ByteBuffer> unused =
          slice
              .getByteBufferFuture()
              .whenComplete((buffer, error) -> sharedBuffer.onSliceComplete(error));
    }
    boolean allDispatched = true;
    for (GcsObjectRange slice : slices) {
      long sliceOffset = slice.getOffset();
      if (!dispatchVectoredRead(
          source,
          ImmutableList.of(slice),
          sliceLength -> sharedBuffer.allocateSlice(sliceOffset, sliceLength))) {
        allDispatched = false;
      }
    }
    return allDispatched && !fetchFuture.isCompletedExceptionally();
  }

  private ImmutableList<GcsObjectRange> createSlices(long startOffset, int length) {
    ImmutableList.Builder<GcsObjectRange> slices = ImmutableList.builder();
    long endOffset = startOffset + length;
    long cursor = startOffset;
    while (cursor < endOffset) {
      int sliceLength = (int) Math.min(maxBlockSizeBytes, endOffset - cursor);
      slices.add(createObjectRange(cursor, sliceLength, new CompletableFuture<>()));
      cursor += sliceLength;
    }
    return slices.build();
  }

  private void promoteSlices(ImmutableList<GcsObjectRange> slices) {
    for (GcsObjectRange slice : slices) {
      inFlightRequests.remove(rangeKeyOf(slice), slice);
      slice.promote();
    }
  }

  private void trackInFlightSlice(GcsObjectRange objectRange) {
    Range<Long> key = rangeKeyOf(objectRange);
    inFlightRequests.put(key, objectRange);
    CompletableFuture<ByteBuffer> unused =
        objectRange
            .getByteBufferFuture()
            .whenComplete((data, error) -> inFlightRequests.remove(key, objectRange));
  }

  private static GcsObjectRange createObjectRange(
      long startOffset, int length, CompletableFuture<ByteBuffer> future) {
    return GcsObjectRange.builder()
        .setOffset(startOffset)
        .setLength(length)
        .setByteBufferFuture(future)
        .build();
  }

  private static Range<Long> rangeKeyOf(GcsObjectRange range) {
    return Range.closedOpen(range.getOffset(), range.getOffset() + range.getLength());
  }

  private static boolean dispatchVectoredRead(
      VectoredSeekableByteChannel source,
      List<GcsObjectRange> rangesToFetch,
      IntFunction<ByteBuffer> allocate) {
    try {
      source.prefetchVectored(rangesToFetch, allocate);
    } catch (IOException | RuntimeException e) {
      for (GcsObjectRange range : rangesToFetch) {
        range.getByteBufferFuture().completeExceptionally(e);
      }
      return false;
    }
    return rangesToFetch.stream()
        .noneMatch(range -> range.getByteBufferFuture().isCompletedExceptionally());
  }

  /**
   * Cancels in-flight requests and evicts cached ranges that this scheduler registered starting
   * inside {@code [startOffset, endOffset)}, leaving ranges registered by other streams untouched.
   */
  void cancelRangeWindow(long startOffset, long endOffset) {
    NavigableMap<Range<Long>, GcsObjectRange> window =
        startingWithin(inFlightRequests, startOffset, endOffset);
    for (GcsObjectRange objectRange : window.values()) {
      objectRange.getByteBufferFuture().cancel(/* mayInterruptIfRunning= */ false);
    }
    window.clear();
    evictRegisteredRanges(startOffset, endOffset);
  }

  private void evictRegisteredRanges(long startOffset, long endOffset) {
    NavigableMap<Range<Long>, CompletableFuture<ByteBuffer>> window =
        startingWithin(registeredRanges, startOffset, endOffset);
    for (Map.Entry<Range<Long>, CompletableFuture<ByteBuffer>> entry : window.entrySet()) {
      Range<Long> range = entry.getKey();
      bufferCache.evictRange(
          itemId, range.lowerEndpoint(), range.upperEndpoint(), entry.getValue());
    }
    window.clear();
  }

  /**
   * Returns the view of {@code ranges} whose start offset lies in {@code [startOffset, endOffset)}.
   */
  private static <V> NavigableMap<Range<Long>, V> startingWithin(
      ConcurrentSkipListMap<Range<Long>, V> ranges, long startOffset, long endOffset) {
    return ranges.subMap(
        Range.closedOpen(startOffset, startOffset),
        true,
        Range.closedOpen(endOffset, endOffset),
        false);
  }

  /** Cancels every outstanding speculative request. */
  void cancelAll() {
    for (GcsObjectRange objectRange : inFlightRequests.values()) {
      objectRange.getByteBufferFuture().cancel(/* mayInterruptIfRunning= */ false);
    }
    inFlightRequests.clear();
  }

  @Override
  public void close() {
    cancelAll();
    registeredStream.close();
  }

  /**
   * Coordinates parallel slice downloads into a single lazily allocated {@link ByteBuffer} so the
   * completed range can be served from {@link PrefetchBufferCache} without stitching or copying.
   */
  private static final class SlicedRangeBuffer {

    private final long baseOffset;
    private final int totalLength;
    private final ImmutableList<GcsObjectRange> slices;
    private final CompletableFuture<ByteBuffer> targetFuture;
    private final AtomicInteger remainingSlices;
    @Nullable private ByteBuffer parentBuffer;

    private SlicedRangeBuffer(
        long baseOffset,
        int totalLength,
        ImmutableList<GcsObjectRange> slices,
        CompletableFuture<ByteBuffer> targetFuture) {
      this.baseOffset = baseOffset;
      this.totalLength = totalLength;
      this.slices = slices;
      this.targetFuture = targetFuture;
      this.remainingSlices = new AtomicInteger(slices.size());
    }

    synchronized ByteBuffer allocateSlice(long sliceOffset, int sliceLength) {
      if (parentBuffer == null) {
        parentBuffer = ByteBuffer.allocate(totalLength);
      }
      int relativeOffset = (int) (sliceOffset - baseOffset);
      ByteBuffer view = parentBuffer.duplicate();
      view.position(relativeOffset);
      view.limit(relativeOffset + sliceLength);
      return view.slice();
    }

    void onSliceComplete(@Nullable Throwable error) {
      if (error != null) {
        if (targetFuture.completeExceptionally(error)) {
          for (GcsObjectRange slice : slices) {
            slice.getByteBufferFuture().cancel(/* mayInterruptIfRunning= */ false);
          }
        }
        return;
      }
      if (remainingSlices.decrementAndGet() == 0) {
        targetFuture.complete(assembleCompletedBuffer());
      }
    }

    private synchronized ByteBuffer assembleCompletedBuffer() {
      if (parentBuffer == null) {
        parentBuffer = ByteBuffer.allocate(totalLength);
      }
      for (GcsObjectRange slice : slices) {
        ByteBuffer sliceBuffer = slice.getByteBufferFuture().join();
        if (!sharesBackingArray(sliceBuffer, parentBuffer)) {
          int relativeOffset = (int) (slice.getOffset() - baseOffset);
          ByteBuffer targetView = parentBuffer.duplicate();
          targetView.position(relativeOffset);
          targetView.limit(relativeOffset + slice.getLength());
          targetView.put(sliceBuffer.duplicate());
        }
      }
      return parentBuffer;
    }

    @SuppressWarnings("ReferenceEquality")
    private static boolean sharesBackingArray(ByteBuffer sliceBuffer, ByteBuffer parent) {
      return sliceBuffer.hasArray() && parent.hasArray() && sliceBuffer.array() == parent.array();
    }
  }
}
