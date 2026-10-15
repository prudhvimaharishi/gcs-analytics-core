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
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.Predicate;
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
 * blocks, avoiding read amplification on unprojected columns. Touching ranges are merged into one
 * range; ranges separated by a gap are fetched separately. Each range goes to the read channel as
 * one request.
 *
 * <p>Instances are bound to a single stream and its reader thread.
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
  private long lastPreReadPosition = -1;
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
    ensureTrackerLoaded(source);
    lastPreReadPosition = -1;
    int requestedLength = dst.remaining();
    boolean coveredInCache =
        tracker != null
            && requestedLength > 0
            && bufferCache.getRangeCovering(itemId, position, 1).isPresent();
    boolean handledBeforeRead =
        coveredInCache && tracker.onBeforeRead(position, requestedLength, scheduleOn(source));
    int servedBytes = bufferCache.serveFromCache(itemId, position, dst, tracker != null);
    if (tracker == null) {
      return servedBytes;
    }
    // A data-page miss is observed in afterRead once the foreground read completes, whereas a
    // dictionary read triggers data prefetch before blocking on dictionary bytes.
    if (servedBytes > 0) {
      if (!handledBeforeRead) {
        tracker.onSingleRead(position, servedBytes, scheduleOn(source));
      }
      return servedBytes;
    }
    if (handledBeforeRead || tracker.onBeforeRead(position, requestedLength, scheduleOn(source))) {
      lastPreReadPosition = position;
    }
    return 0;
  }

  @Override
  public void afterRead(long position, int bytesRead, VectoredSeekableByteChannel source)
      throws IOException {
    ensureTrackerLoaded(source);
    if (position == lastPreReadPosition) {
      lastPreReadPosition = -1;
      return;
    }
    if (tracker != null) {
      tracker.onSingleRead(position, bytesRead, scheduleOn(source));
    }
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source)
      throws IOException {
    ensureTrackerLoaded(source);
    List<GcsObjectRange> unservedRanges =
        bufferCache.serveVectoredFromCache(itemId, ranges, allocate, source, tracker != null);
    if (tracker != null) {
      tracker.onVectoredRead(ranges);
    }
    return unservedRanges;
  }

  @Override
  public void onClose() {
    if (tracker != null) {
      tracker.recordOutcomeOnClose();
    }
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private void ensureTrackerLoaded(VectoredSeekableByteChannel source) throws IOException {
    if (tracker != null) {
      return;
    }
    Optional<ParquetFileLayout> layout =
        findCachedFooter(source).flatMap(ParquetFooterParser::parse);
    if (!layout.isPresent()) {
      return;
    }
    fileSize = source.size();
    tracker =
        new RowGroupPrefetchTracker(
            layout.get(),
            cacheManager.getSchemaAccessHistory().get(),
            prefetchOptions.getDictionaryTrigger(),
            scheduler::cancelRangeWindow);
  }

  /** Returns a callback that schedules speculative reads of byte ranges on {@code source}. */
  private Predicate<ImmutableList<Range<Long>>> scheduleOn(VectoredSeekableByteChannel source) {
    return ranges -> scheduler.schedule(source, ranges, fileSize);
  }

  /**
   * Returns the cached footer of this object. A stream opened without a generation finds it under
   * the generation-qualified id once the channel learns the generation, because that is the id the
   * footer optimizer caches it under.
   */
  private Optional<ByteBuffer> findCachedFooter(VectoredSeekableByteChannel source) {
    Optional<ByteBuffer> footer = cacheManager.getFooter(itemId);
    GcsItemInfo itemInfo = source.getItemInfo();
    if (footer.isPresent() || itemInfo == null) {
      return footer;
    }
    return cacheManager.getFooter(itemInfo.getItemId());
  }
}
