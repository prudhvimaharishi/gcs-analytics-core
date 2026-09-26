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

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link FormatOptimizer} that speculatively fetches exact Parquet column chunks and dictionary
 * pages before the engine asks for them.
 *
 * <p>Footer I/O is owned exclusively by {@code GcsFooterOptimizer}; this optimizer consumes the
 * footer published in {@link AnalyticsCacheManager}. If no footer is present in the global cache,
 * prefetching is skipped.
 *
 * <p>Speculation schedules exact column chunk and dictionary page byte ranges rather than fixed
 * blocks, avoiding read amplification on unprojected columns. Touching ranges are merged while the
 * merged span stays within {@link GcsPrefetchOptions#getBlockSizeBytes()}; ranges separated by a
 * gap are fetched separately.
 *
 * <p>Instances are bound to a single stream and its reader thread. Deferred row-group speculation
 * and {@link #onClose()} synchronize on the instance so that no speculation is scheduled after the
 * stream closes.
 */
public final class PredictivePrefetchOptimizer implements FormatOptimizer {

  private static final String PARQUET_EXTENSION = ".parquet";
  private static final String PREFETCH_DISABLED_IN_CACHE_MANAGER =
      "Predictive prefetching is disabled in the cache manager";

  private final GcsPrefetchOptions prefetchOptions;

  private GcsItemId itemId;
  private AnalyticsCacheManager cacheManager;
  private PrefetchBufferCache bufferCache;
  private PrefetchScheduler scheduler;
  private long fileSize = -1;
  private volatile boolean closed;
  @Nullable private RowGroupPrefetchTracker tracker;

  public PredictivePrefetchOptimizer(GcsPrefetchOptions prefetchOptions) {
    this.prefetchOptions = checkNotNull(prefetchOptions, "prefetchOptions cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return prefetchOptions.isEnabled()
        && itemId
            .getObjectName()
            .map(name -> name.toLowerCase().endsWith(PARQUET_EXTENSION))
            .orElse(false);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.bufferCache =
        cacheManager
            .getPrefetchBufferCache()
            .orElseThrow(() -> new IllegalStateException(PREFETCH_DISABLED_IN_CACHE_MANAGER));
    this.itemId = itemId;
    this.cacheManager = cacheManager;
    this.scheduler = new PrefetchScheduler(itemId, bufferCache);
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureTrackerLoaded();
    boolean evictConsumed = tracker == null || tracker.touchesDataPages(position, dst.remaining());
    int servedBytes =
        bufferCache.serveFromCache(itemId, position, dst, tracker != null, evictConsumed);
    // A miss is observed in afterRead once the foreground read completes, so speculation never
    // competes with the read the caller is blocked on.
    if (tracker != null && servedBytes > 0) {
      tracker.onSingleRead(
          position, servedBytes, ranges -> scheduler.schedule(source, ranges, fileSize));
    }
    return servedBytes;
  }

  @Override
  public void afterRead(long position, int bytesRead, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureTrackerLoaded();
    if (tracker != null) {
      tracker.onSingleRead(
          position, bytesRead, ranges -> scheduler.schedule(source, ranges, fileSize));
    }
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureTrackerLoaded();
    List<GcsObjectRange> unservedRanges =
        bufferCache.serveVectoredFromCache(itemId, ranges, allocate, source, tracker != null);
    if (tracker != null) {
      tracker.onVectoredRead(ranges);
    }
    return unservedRanges;
  }

  @Override
  public void afterReadVectored(List<GcsObjectRange> ranges, VectoredSeekableByteChannel source) {
    ensureTrackerLoaded();
    if (tracker == null) {
      return;
    }
    OptionalInt targetRowGroup =
        tracker.pollPendingNextRowGroup(
            toSchedule -> scheduler.schedule(source, toSchedule, fileSize));
    if (!targetRowGroup.isPresent()) {
      return;
    }
    int targetIndex = targetRowGroup.getAsInt();
    CompletableFuture<?>[] rangeFutures =
        ranges.stream()
            .map(GcsObjectRange::getByteBufferFuture)
            .toArray(CompletableFuture<?>[]::new);
    CompletableFuture<Void> unused =
        CompletableFuture.allOf(rangeFutures)
            .whenComplete(
                (result, error) -> {
                  if (error == null) {
                    prefetchRowGroupIfOpen(source, targetIndex);
                  }
                });
  }

  @Override
  public synchronized void onClose() {
    closed = true;
    if (tracker != null) {
      tracker.recordOutcomeOnClose();
    }
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private synchronized void prefetchRowGroupIfOpen(
      VectoredSeekableByteChannel source, int rowGroupIndex) {
    if (!closed && tracker != null) {
      tracker.prefetchRowGroup(
          rowGroupIndex, ranges -> scheduler.schedule(source, ranges, fileSize));
    }
  }

  private void ensureTrackerLoaded() {
    if (tracker != null) {
      return;
    }
    cacheManager
        .getFooter(itemId)
        .flatMap(ParquetFooterParser::parse)
        .ifPresent(
            layout ->
                tracker =
                    new RowGroupPrefetchTracker(
                        layout,
                        cacheManager.getSchemaAccessHistory().get(),
                        prefetchOptions.getBlockSizeBytes(),
                        prefetchOptions.getDictionaryTrigger(),
                        scheduler::cancelRangeWindow));
  }
}
