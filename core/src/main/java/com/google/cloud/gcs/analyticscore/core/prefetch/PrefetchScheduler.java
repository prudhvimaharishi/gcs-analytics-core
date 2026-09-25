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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Issues speculative reads for exact byte ranges and registers their futures directly in {@link
 * PrefetchBufferCache}.
 *
 * <p>Ranges already present or in-flight in {@link PrefetchBufferCache} are skipped automatically.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  private final PrefetchBufferCache bufferCache;
  private final Telemetry telemetry;
  private final int maxConcurrentRanges;
  private final ConcurrentSkipListMap<Long, GcsObjectRange> inFlightRequests =
      new ConcurrentSkipListMap<>();
  private final java.util.Set<Long> scheduledOffsets =
      java.util.concurrent.ConcurrentHashMap.newKeySet();
  private volatile GcsItemId scheduledItemId;

  PrefetchScheduler(PrefetchBufferCache bufferCache, Telemetry telemetry, int maxConcurrentRanges) {
    this.bufferCache = checkNotNull(bufferCache, "bufferCache cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
    this.maxConcurrentRanges = maxConcurrentRanges;
  }

  /**
   * Schedules speculative reads for the given byte ranges, ignoring any that are already covered in
   * the cache or beyond the concurrency budget.
   *
   * @param byteRanges exact closed-open byte ranges {@code [startOffset, endOffset)} in ascending
   *     order
   * @param fileSize the size of the object, used to truncate ranges extending past EOF
   */
  long schedule(
      VectoredSeekableByteChannel source,
      GcsItemId itemId,
      Collection<Range<Long>> byteRanges,
      long fileSize) {
    this.scheduledItemId = itemId;
    List<GcsObjectRange> rangesToFetch = new ArrayList<>();
    long scheduledBytes = 0L;
    for (Range<Long> byteRange : byteRanges) {
      if (inFlightRequests.size() >= maxConcurrentRanges) {
        break;
      }
      long startOffset = byteRange.lowerEndpoint();
      long endOffset = Math.min(byteRange.upperEndpoint(), fileSize);
      int length = (int) (endOffset - startOffset);
      if (length <= 0) {
        continue;
      }
      CompletableFuture<ByteBuffer> request = new CompletableFuture<>();
      CompletableFuture<ByteBuffer> cachedFuture =
          request.thenApply(
              data -> {
                telemetry.recordMetric(
                    Metric.PREFETCH_BYTES_LOADED, data.remaining(), Collections.emptyMap());
                return data.duplicate();
              });
      GcsObjectRange objectRange =
          GcsObjectRange.builder()
              .setOffset(startOffset)
              .setLength(length)
              .setByteBufferFuture(request)
              .build();
      if (!bufferCache.registerRange(
          itemId, startOffset, length, cachedFuture, objectRange::promote)) {
        continue;
      }
      scheduledBytes += length;
      scheduledOffsets.add(startOffset);
      inFlightRequests.put(startOffset, objectRange);
      CompletableFuture<ByteBuffer> unused =
          request.whenComplete((data, error) -> inFlightRequests.remove(startOffset));
      rangesToFetch.add(objectRange);
    }

    if (rangesToFetch.isEmpty()) {
      return 0L;
    }

    try {
      source.prefetchVectored(rangesToFetch, ByteBuffer::allocate);
    } catch (IOException | RuntimeException e) {
      for (GcsObjectRange range : rangesToFetch) {
        range.getByteBufferFuture().completeExceptionally(e);
      }
    }
    return scheduledBytes;
  }

  /**
   * Cancels in-flight requests that no foreground reader has promoted and evicts cached ranges
   * inside {@code [startOffset, endOffset)}.
   */
  void cancelRangeWindow(long startOffset, long endOffset) {
    for (java.util.Map.Entry<Long, GcsObjectRange> entry :
        new ArrayList<>(inFlightRequests.subMap(startOffset, true, endOffset, false).entrySet())) {
      cancelUnlessPromoted(entry.getValue());
      inFlightRequests.remove(entry.getKey());
    }
    GcsItemId item = scheduledItemId;
    if (item != null) {
      for (Long offset : new ArrayList<>(scheduledOffsets)) {
        if (offset >= startOffset && offset < endOffset) {
          bufferCache.evictRange(item, offset);
          scheduledOffsets.remove(offset);
        }
      }
    }
  }

  /**
   * Cancels every outstanding speculative request that no foreground reader has promoted, and
   * evicts unconsumed stream ranges.
   */
  void cancelAll() {
    for (GcsObjectRange objectRange : inFlightRequests.values()) {
      cancelUnlessPromoted(objectRange);
    }
    inFlightRequests.clear();
    GcsItemId item = scheduledItemId;
    if (item != null) {
      for (Long offset : scheduledOffsets) {
        bufferCache.evictRange(item, offset);
      }
    }
    scheduledOffsets.clear();
  }

  /**
   * Cancels {@code objectRange}'s download unless a foreground reader promoted it, because that
   * reader is waiting on the same shared future.
   */
  private static void cancelUnlessPromoted(GcsObjectRange objectRange) {
    if (objectRange.isPromoted()) {
      return;
    }
    objectRange.getByteBufferFuture().cancel(/* mayInterruptIfRunning= */ false);
    objectRange.cancel();
  }

  @Override
  public void close() {
    cancelAll();
  }

  /** Returns the start offsets of ranges whose speculative request has not settled yet. */
  ImmutableList<Long> getInFlightOffsets() {
    return ImmutableList.copyOf(inFlightRequests.keySet());
  }
}
