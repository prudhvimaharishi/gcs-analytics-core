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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Issues speculative reads for whole blocks and publishes the results into a {@link
 * PrefetchBufferCache}.
 *
 * <p>Every eligible block becomes its own range in a single {@link
 * VectoredSeekableByteChannel#readVectored} call. Contiguous blocks are therefore merged into one
 * GCS request by the channel itself, and the response is split back per block, so this class does
 * no coalescing of its own.
 *
 * <p>Dispatching through {@code readVectored} also matters because the channel the engine is
 * reading from carries a mutable position and is not safe to use concurrently; issuing speculation
 * any other way would corrupt in-progress reads.
 *
 * <p>Blocks already cached or already claimed are skipped, so repeatedly asking for the same
 * speculation is harmless.
 *
 * <p>This class is thread-safe.
 */
final class PrefetchScheduler implements AutoCloseable {

  private final PrefetchBufferCache bufferCache;
  private final Telemetry telemetry;
  private final int maxConcurrentBlocks;
  private final Map<Long, CompletableFuture<ByteBuffer>> inFlightByOffset =
      new ConcurrentHashMap<>();

  PrefetchScheduler(PrefetchBufferCache bufferCache, Telemetry telemetry, int maxConcurrentBlocks) {
    this.bufferCache = checkNotNull(bufferCache, "bufferCache cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
    this.maxConcurrentBlocks = maxConcurrentBlocks;
  }

  /**
   * Schedules the blocks starting at the given offsets, ignoring any that are cached, claimed, or
   * beyond the concurrency budget.
   *
   * <p>Failures are swallowed: speculation is best effort, and a failed prefetch simply becomes a
   * synchronous read later.
   *
   * @param blockOffsets block-aligned start offsets, in ascending order
   * @param fileSize the size of the object, used to truncate the final block
   */
  void schedule(
      VectoredSeekableByteChannel source,
      GcsItemId itemId,
      Collection<Long> blockOffsets,
      long fileSize) {
    List<GcsObjectRange> ranges = new ArrayList<>();
    List<Long> claimedOffsets = new ArrayList<>();
    for (long blockOffset : blockOffsets) {
      if (inFlightByOffset.size() >= maxConcurrentBlocks) {
        break;
      }
      int blockLength = blockLength(blockOffset, fileSize);
      if (blockLength <= 0 || !shouldFetch(itemId, blockOffset)) {
        continue;
      }
      ranges.add(createRange(itemId, blockOffset, blockLength));
      claimedOffsets.add(blockOffset);
    }

    if (ranges.isEmpty()) {
      return;
    }

    try {
      source.readVectored(ranges, ByteBuffer::allocate);
    } catch (IOException | RuntimeException e) {
      for (long blockOffset : claimedOffsets) {
        abandon(itemId, blockOffset);
      }
    }
  }

  /** Cancels every outstanding speculative request. */
  void cancelAll() {
    for (CompletableFuture<ByteBuffer> pending : inFlightByOffset.values()) {
      pending.cancel(/* mayInterruptIfRunning= */ false);
    }
    inFlightByOffset.clear();
  }

  @Override
  public void close() {
    cancelAll();
  }

  /** Returns the offsets of blocks whose speculative request has not settled yet. */
  ImmutableList<Long> getInFlightOffsets() {
    return ImmutableList.copyOf(inFlightByOffset.keySet());
  }

  private int blockLength(long blockOffset, long fileSize) {
    return (int) Math.min(bufferCache.getBlockSizeBytes(), fileSize - blockOffset);
  }

  private boolean shouldFetch(GcsItemId itemId, long blockOffset) {
    if (bufferCache.isCached(itemId, blockOffset)) {
      return false;
    }
    return bufferCache.tryClaim(itemId, blockOffset);
  }

  private GcsObjectRange createRange(GcsItemId itemId, long blockOffset, int blockLength) {
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    inFlightByOffset.put(blockOffset, future);
    CompletableFuture<ByteBuffer> unusedPublishStage =
        future.whenComplete((data, error) -> publish(itemId, blockOffset, data, error));
    return GcsObjectRange.builder()
        .setOffset(blockOffset)
        .setLength(blockLength)
        .setByteBufferFuture(future)
        .build();
  }

  private void publish(GcsItemId itemId, long blockOffset, ByteBuffer data, Throwable error) {
    inFlightByOffset.remove(blockOffset);
    if (error != null || data == null) {
      bufferCache.releaseClaim(itemId, blockOffset);
      return;
    }
    int loadedBytes = data.remaining();
    bufferCache.putRange(itemId, blockOffset, data);
    bufferCache.releaseClaim(itemId, blockOffset);
    telemetry.recordMetric(Metric.PREFETCH_BYTES_LOADED, loadedBytes, Collections.emptyMap());
  }

  private void abandon(GcsItemId itemId, long blockOffset) {
    inFlightByOffset.remove(blockOffset);
    bufferCache.releaseClaim(itemId, blockOffset);
  }
}
