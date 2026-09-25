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
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link FormatOptimizer} that speculatively fetches exact Parquet column chunks before the
 * engine asks for them.
 *
 * <p>Footer I/O is owned exclusively by {@code GcsFooterOptimizer}; this optimizer consumes the
 * footer published in {@link AnalyticsCacheManager}. If no footer is present in the global cache,
 * prefetching is skipped.
 *
 * <p>Speculation schedules exact column chunk byte ranges rather than fixed blocks, avoiding read
 * amplification on unprojected columns while allowing the underlying channel to coalesce nearby
 * ranges.
 *
 * <p>Instances are bound to a single stream and its reader thread. Row-group speculation and {@link
 * #onClose()} synchronize on the instance so that no speculation is scheduled after the stream
 * closes.
 */
public final class PredictivePrefetchOptimizer implements FormatOptimizer {

  private static final String PARQUET_EXTENSION = ".parquet";
  private static final int MAX_CONCURRENT_PREFETCH_RANGES = 32;

  private final GcsPrefetchOptions prefetchOptions;
  private final Telemetry telemetry;

  private GcsItemId itemId;
  private AnalyticsCacheManager cacheManager;
  private PrefetchBufferCache bufferCache;
  private SchemaAccessHistory accessHistory;
  private PrefetchScheduler scheduler;
  @Nullable private VectoredSeekableByteChannel sourceChannel;

  private long fileSize = -1;
  private volatile boolean closed;
  private boolean firstRowGroupPrefetched;
  @Nullable private ParquetFileLayout layout;
  private long[] rowGroupStartOffsets = new long[0];
  private long[] rowGroupEndOffsets = new long[0];
  private int pendingSpeculationRowGroupOrdinal = -1;

  public PredictivePrefetchOptimizer(GcsPrefetchOptions prefetchOptions, Telemetry telemetry) {
    this.prefetchOptions = checkNotNull(prefetchOptions, "prefetchOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
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
    this.itemId = itemId;
    this.cacheManager = cacheManager;
    this.bufferCache = cacheManager.getPrefetchBufferCache();
    this.accessHistory = cacheManager.getSchemaAccessHistory();
    this.scheduler = new PrefetchScheduler(bufferCache, telemetry, MAX_CONCURRENT_PREFETCH_RANGES);
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    onOpen(fileInfo.getItemInfo().getItemId(), cacheManager);
    this.fileSize = fileInfo.getItemInfo().getSize();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    this.sourceChannel = source;
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureLayoutLoaded();
    int servedBytes = serveFromCache(position, dst);

    // A miss is observed in afterRead once the foreground read completes, so speculation never
    // competes with the read the caller is blocked on.
    if (layout != null && servedBytes > 0) {
      observeAccess(source, position, servedBytes);
    }
    return servedBytes;
  }

  @Override
  public void afterRead(long position, int bytesRead, VectoredSeekableByteChannel source)
      throws IOException {
    this.sourceChannel = source;
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureLayoutLoaded();
    if (layout != null) {
      observeAccess(source, position, bytesRead);
    }
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) {
    List<GcsObjectRange> unservedRanges = new ArrayList<>();
    for (GcsObjectRange range : ranges) {
      if (!tryCompleteFromCache(range, allocate)) {
        unservedRanges.add(range);
      }
    }
    return ImmutableList.copyOf(unservedRanges);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source)
      throws IOException {
    this.sourceChannel = source;
    if (fileSize < 0) {
      fileSize = source.size();
    }
    ensureLayoutLoaded();
    List<GcsObjectRange> unservedRanges = readVectored(ranges, allocate);

    if (layout != null) {
      recordVectoredAccess(ranges);
    }
    return unservedRanges;
  }

  @Override
  public void afterReadVectored(List<GcsObjectRange> ranges, VectoredSeekableByteChannel source) {
    ensureLayoutLoaded();
    if (layout == null) {
      return;
    }
    if (pendingSpeculationRowGroupOrdinal < 0) {
      prefetchFirstRowGroupOnce(source);
      return;
    }
    firstRowGroupPrefetched = true;
    int targetOrdinal = pendingSpeculationRowGroupOrdinal;
    pendingSpeculationRowGroupOrdinal = -1;
    if (!shouldSpeculateNextRowGroup(targetOrdinal)) {
      return;
    }
    CompletableFuture<?>[] rangeFutures =
        ranges.stream()
            .map(GcsObjectRange::getByteBufferFuture)
            .toArray(CompletableFuture<?>[]::new);
    CompletableFuture<Void> unused =
        CompletableFuture.allOf(rangeFutures)
            .whenComplete(
                (result, error) -> {
                  if (!closed && error == null) {
                    long ignored = speculateRowGroup(source, targetOrdinal);
                  }
                });
  }

  private boolean shouldSpeculateNextRowGroup(int targetOrdinal) {
    return targetOrdinal >= 0 && targetOrdinal < rowGroupStartOffsets.length;
  }

  /**
   * Closes the scheduler. Synchronized with {@code speculateRowGroup} so that a speculation that
   * already passed its {@code closed} check cannot schedule ranges after the scheduler is closed.
   */
  @Override
  public synchronized void onClose() {
    closed = true;
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private int serveFromCache(long position, ByteBuffer dst) {
    int servedBytes = bufferCache.copyInto(itemId, position, dst);
    if (servedBytes == 0) {
      recordCacheMiss();
      return 0;
    }
    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    telemetry.recordMetric(Metric.PREFETCH_BYTES_CONSUMED, servedBytes, Collections.emptyMap());
    return servedBytes;
  }

  /**
   * Records a cache miss only when the footer layout is known, because without it this optimizer
   * could never have prefetched the requested bytes.
   */
  private void recordCacheMiss() {
    if (layout != null) {
      telemetry.recordMetric(Metric.PREFETCH_CACHE_MISS, 1L, Collections.emptyMap());
    }
  }

  /**
   * Takes ownership of {@code range} when its bytes are cached or already being fetched, and
   * returns whether the caller can stop treating it as outstanding.
   */
  private boolean tryCompleteFromCache(GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    if (bufferCache == null) {
      return false;
    }
    Optional<CompletableFuture<ByteBuffer>> populatedFuture =
        bufferCache.consumeRange(itemId, range.getOffset(), range.getLength(), allocate);
    if (!populatedFuture.isPresent()) {
      recordCacheMiss();
      return false;
    }
    CompletableFuture<ByteBuffer> unused =
        populatedFuture
            .get()
            .whenComplete(
                (target, error) -> {
                  if (error == null && target != null) {
                    range.getByteBufferFuture().complete(target);
                    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
                    telemetry.recordMetric(
                        Metric.PREFETCH_BYTES_CONSUMED, range.getLength(), Collections.emptyMap());
                    return;
                  }
                  recordCacheMiss();
                  fallbackToSourceRead(range, allocate);
                });
    return true;
  }

  private void fallbackToSourceRead(GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    VectoredSeekableByteChannel source = sourceChannel;
    if (source == null) {
      range
          .getByteBufferFuture()
          .completeExceptionally(
              new IOException(
                  String.format("No channel available to read range %s of %s", range, itemId)));
      return;
    }
    try {
      source.readVectored(ImmutableList.of(range), allocate);
    } catch (IOException | RuntimeException e) {
      range.getByteBufferFuture().completeExceptionally(e);
    }
  }

  /** Records the columns this read touched and speculates ahead of the current read end offset. */
  private void observeAccess(
      VectoredSeekableByteChannel source, long position, int requestedLength) {
    int rowGroupOrdinal = findRowGroupOrdinal(position);
    if (rowGroupOrdinal < 0) {
      prefetchFirstRowGroupOnce(source);
      return;
    }
    firstRowGroupPrefetched = true;
    long currentReadEnd = position + requestedLength;
    recordColumnsInRange(rowGroupOrdinal, position, currentReadEnd);
    bufferCache.evictConsumedRanges(itemId, position, currentReadEnd);
    speculateRowGroups(source, rowGroupOrdinal, currentReadEnd);
  }

  /** Records the columns covered by {@code ranges} and notes the row group to speculate next. */
  private void recordVectoredAccess(List<GcsObjectRange> ranges) {
    int lastRowGroupOrdinal = -1;
    for (GcsObjectRange range : ranges) {
      int rowGroupOrdinal = findRowGroupOrdinal(range.getOffset());
      if (rowGroupOrdinal < 0) {
        continue;
      }
      recordColumnsInRange(
          rowGroupOrdinal, range.getOffset(), range.getOffset() + range.getLength());
      lastRowGroupOrdinal = Math.max(lastRowGroupOrdinal, rowGroupOrdinal);
    }
    if (lastRowGroupOrdinal < 0) {
      return;
    }
    firstRowGroupPrefetched = true;
    pendingSpeculationRowGroupOrdinal = lastRowGroupOrdinal + 1;
  }

  /** Records every column of {@code rowGroupOrdinal} whose bytes fall in the given byte range. */
  private void recordColumnsInRange(int rowGroupOrdinal, long startOffset, long endOffset) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return;
    }
    int fingerprint = layout.getSchemaFingerprint();
    for (ParquetColumnChunk chunk : rowGroup.get().getColumnChunks().values()) {
      if (overlaps(chunk.getStartOffset(), chunk.getEndOffset(), startOffset, endOffset)) {
        accessHistory.recordDataAccess(fingerprint, chunk.getColumnPath());
      }
    }
  }

  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < otherEnd && end > otherStart;
  }

  /** Prefetches the first row group's known columns once, as soon as the footer has been read. */
  private void prefetchFirstRowGroupOnce(VectoredSeekableByteChannel source) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    long ignored = speculateRowGroup(source, 0);
  }

  /**
   * Fetches the columns already known for this schema in the current row group (beyond {@code
   * currentReadEnd}) and the next surviving row group.
   */
  private void speculateRowGroups(
      VectoredSeekableByteChannel source, int rowGroupOrdinal, long currentReadEnd) {
    int nextOrdinal = rowGroupOrdinal + 1;
    OptionalInt targetNextOrdinal =
        shouldSpeculateNextRowGroup(nextOrdinal)
            ? OptionalInt.of(nextOrdinal)
            : OptionalInt.empty();
    List<Range<Long>> learnedRanges =
        collectLearnedRangesForRowGroups(rowGroupOrdinal, targetNextOrdinal);
    List<Range<Long>> rangesAhead = new ArrayList<>();
    for (Range<Long> range : learnedRanges) {
      if (range.upperEndpoint() <= currentReadEnd) {
        continue;
      }
      if (range.lowerEndpoint() < currentReadEnd) {
        rangesAhead.add(Range.closedOpen(currentReadEnd, range.upperEndpoint()));
      } else {
        rangesAhead.add(range);
      }
    }
    if (rangesAhead.isEmpty()) {
      return;
    }
    long ignored = scheduler.schedule(source, itemId, rangesAhead, fileSize);
  }

  /** Fetches the columns already known for this schema in a single row group. */
  private synchronized long speculateRowGroup(
      VectoredSeekableByteChannel source, int rowGroupOrdinal) {
    if (closed || layout == null) {
      return 0L;
    }
    List<Range<Long>> ranges =
        collectLearnedRangesForRowGroups(rowGroupOrdinal, OptionalInt.empty());
    if (ranges.isEmpty()) {
      return 0L;
    }
    return scheduler.schedule(source, itemId, ranges, fileSize);
  }

  /**
   * Returns the sorted exact byte ranges covering the columns known for this schema in the current
   * row group and, when present, the next surviving row group.
   */
  private List<Range<Long>> collectLearnedRangesForRowGroups(
      int currentRowGroupOrdinal, OptionalInt nextRowGroupOrdinal) {
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(layout.getSchemaFingerprint());
    if (dataColumns.isEmpty()) {
      return new ArrayList<>();
    }
    List<Range<Long>> ranges =
        new ArrayList<>(collectCoalescedRowGroupRanges(currentRowGroupOrdinal, dataColumns));
    if (nextRowGroupOrdinal.isPresent()) {
      ranges.addAll(collectCoalescedRowGroupRanges(nextRowGroupOrdinal.getAsInt(), dataColumns));
    }
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return ranges;
  }

  private List<Range<Long>> collectCoalescedRowGroupRanges(
      int rowGroupOrdinal, Set<String> dataColumns) {
    List<Range<Long>> ranges = new ArrayList<>();
    collectRowGroupRanges(ranges, rowGroupOrdinal, dataColumns);
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return coalesceConnectedRanges(ranges);
  }

  /**
   * Merges touching or overlapping sorted ranges and splits any span exceeding {@link
   * GcsPrefetchOptions#getBlockSizeBytes()} so adjacent columns download in parallel without
   * humongous buffers.
   */
  private List<Range<Long>> coalesceConnectedRanges(List<Range<Long>> sortedRanges) {
    if (sortedRanges.isEmpty()) {
      return sortedRanges;
    }
    long maxRangeBytes = prefetchOptions.getBlockSizeBytes();
    List<Range<Long>> coalesced = new ArrayList<>(sortedRanges.size());
    Range<Long> current = sortedRanges.get(0);
    for (int i = 1; i < sortedRanges.size(); i++) {
      Range<Long> next = sortedRanges.get(i);
      if (current.isConnected(next)) {
        current = current.span(next);
      } else {
        appendBoundedSlices(coalesced, current, maxRangeBytes);
        current = next;
      }
    }
    appendBoundedSlices(coalesced, current, maxRangeBytes);
    return coalesced;
  }

  private static void appendBoundedSlices(
      List<Range<Long>> target, Range<Long> span, long maxRangeBytes) {
    long start = span.lowerEndpoint();
    long end = span.upperEndpoint();
    while (end - start > maxRangeBytes) {
      long chunkEnd = start + maxRangeBytes;
      target.add(Range.closedOpen(start, chunkEnd));
      start = chunkEnd;
    }
    if (end > start) {
      target.add(Range.closedOpen(start, end));
    }
  }

  private void collectRowGroupRanges(
      List<Range<Long>> ranges, int rowGroupOrdinal, Set<String> dataColumns) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return;
    }
    for (String columnPath : dataColumns) {
      rowGroup
          .get()
          .getColumnChunk(columnPath)
          .ifPresent(
              chunk -> ranges.add(Range.closedOpen(chunk.getStartOffset(), chunk.getEndOffset())));
    }
  }

  /** Returns the ordinal of the row group containing {@code position}, or {@code -1} if none. */
  private int findRowGroupOrdinal(long position) {
    if (rowGroupStartOffsets.length == 0 || position < rowGroupStartOffsets[0]) {
      return -1;
    }
    int searchResult = Arrays.binarySearch(rowGroupStartOffsets, position);
    int ordinal = searchResult >= 0 ? searchResult : -searchResult - 2;
    return position < rowGroupEndOffsets[ordinal] ? ordinal : -1;
  }

  private void ensureLayoutLoaded() {
    if (layout != null) {
      return;
    }
    Optional<ByteBuffer> sharedTail = cacheManager.getFooter(itemId);
    if (!sharedTail.isPresent()) {
      return;
    }
    layout = ParquetFooterParser.parse(sharedTail.get(), fileSize).orElse(null);
    cacheRowGroupBoundaries();
  }

  /**
   * Snapshots the row group byte ranges once, because deriving them from the column chunks is
   * linear in the number of columns and the lookup runs on every read.
   */
  private void cacheRowGroupBoundaries() {
    if (layout == null) {
      rowGroupStartOffsets = new long[0];
      rowGroupEndOffsets = new long[0];
      return;
    }
    int rowGroupCount = layout.getRowGroups().size();
    rowGroupStartOffsets = new long[rowGroupCount];
    rowGroupEndOffsets = new long[rowGroupCount];
    for (int ordinal = 0; ordinal < rowGroupCount; ordinal++) {
      ParquetRowGroup rowGroup = layout.getRowGroups().get(ordinal);
      rowGroupStartOffsets[ordinal] = rowGroup.getStartOffset();
      rowGroupEndOffsets[ordinal] = rowGroup.getEndOffset();
    }
  }
}
