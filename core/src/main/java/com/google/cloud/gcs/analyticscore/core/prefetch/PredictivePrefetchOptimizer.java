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
import com.google.common.collect.Sets;
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
import java.util.concurrent.ConcurrentHashMap;
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
 * blocks, avoiding read amplification on unprojected columns. Touching ranges are merged into spans
 * of at most {@link GcsPrefetchOptions#getBlockSizeBytes()}; ranges separated by a gap are fetched
 * separately.
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
  private final Set<Integer> rowGroupsWithDataPrefetch = ConcurrentHashMap.newKeySet();
  private volatile ImmutableSet<String> prefetchedDictionaryColumns = ImmutableSet.of();
  @Nullable private ParquetFileLayout layout;
  private long[] rowGroupStartOffsets = new long[0];
  private long[] rowGroupEndOffsets = new long[0];
  private int pendingSpeculationRowGroupOrdinal = -1;
  private RowGroupFilterTracker filterTracker = new RowGroupFilterTracker();

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
    this.filterTracker = new RowGroupFilterTracker();
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
    if (recordColumnsInRange(rowGroupOrdinal, position, currentReadEnd)) {
      bufferCache.evictConsumedRanges(itemId, position, currentReadEnd);
      speculateRowGroups(source, rowGroupOrdinal, currentReadEnd);
    } else {
      onDictionaryPageRead(source, rowGroupOrdinal);
    }
  }

  /**
   * Prefetches dictionary pages the footer did not already cover and, once the configured
   * dictionary trigger is met for {@code rowGroupOrdinal}, prefetches that row group's data pages.
   */
  private void onDictionaryPageRead(VectoredSeekableByteChannel source, int rowGroupOrdinal) {
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (!prefetchedDictionaryColumns.containsAll(dictionaryColumns)) {
      speculateDictionaryPagesFrom(source, rowGroupOrdinal + 1);
    }
    if (!rowGroupsWithDataPrefetch.contains(rowGroupOrdinal)
        && isDictionaryTriggerMet(rowGroupOrdinal, dictionaryColumns)
        && !hasPendingDataPrefetchBefore(rowGroupOrdinal)
        && speculateRowGroupDataPages(source, rowGroupOrdinal)) {
      rowGroupsWithDataPrefetch.add(rowGroupOrdinal);
    }
  }

  /**
   * Returns whether an earlier row group still has a data prefetch waiting for its data read, so
   * that a dictionary sweep keeps only the front row group's data in memory.
   */
  private boolean hasPendingDataPrefetchBefore(int rowGroupOrdinal) {
    int lastDataReadOrdinal = filterTracker.getLastDataReadOrdinal();
    for (int prefetchedOrdinal : rowGroupsWithDataPrefetch) {
      if (prefetchedOrdinal > lastDataReadOrdinal && prefetchedOrdinal < rowGroupOrdinal) {
        return true;
      }
    }
    return false;
  }

  private boolean isDictionaryTriggerMet(int rowGroupOrdinal, Set<String> dictionaryColumns) {
    switch (prefetchOptions.getDictionaryTrigger()) {
      case FIRST_DICT_READ:
        return filterTracker.hasReadAnyDictionary(rowGroupOrdinal, dictionaryColumns);
      case LAST_DICT_READ:
        return filterTracker.hasReadAllDictionaries(layout, rowGroupOrdinal, dictionaryColumns);
    }
    throw new IllegalStateException(
        "Unknown dictionary trigger: " + prefetchOptions.getDictionaryTrigger());
  }

  /** Records the columns covered by {@code ranges} and notes the row group to speculate next. */
  private void recordVectoredAccess(List<GcsObjectRange> ranges) {
    int lastRowGroupOrdinal = -1;
    boolean touchedAnyDataPage = false;
    for (GcsObjectRange range : ranges) {
      int rowGroupOrdinal = findRowGroupOrdinal(range.getOffset());
      if (rowGroupOrdinal < 0) {
        continue;
      }
      if (recordColumnsInRange(
          rowGroupOrdinal, range.getOffset(), range.getOffset() + range.getLength())) {
        touchedAnyDataPage = true;
        lastRowGroupOrdinal = Math.max(lastRowGroupOrdinal, rowGroupOrdinal);
      }
    }
    if (!touchedAnyDataPage || lastRowGroupOrdinal < 0) {
      return;
    }
    firstRowGroupPrefetched = true;
    pendingSpeculationRowGroupOrdinal = lastRowGroupOrdinal + 1;
  }

  /**
   * Records every column of {@code rowGroupOrdinal} whose bytes fall in the given byte range, and
   * returns {@code true} if at least one column's data pages were touched.
   */
  private boolean recordColumnsInRange(int rowGroupOrdinal, long startOffset, long endOffset) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return false;
    }
    int fingerprint = layout.getSchemaFingerprint();
    boolean touchedDataPage = false;
    for (ParquetColumnChunk chunk : rowGroup.get().getColumnChunks().values()) {
      if (overlaps(chunk.getDataPageOffset(), chunk.getEndOffset(), startOffset, endOffset)) {
        accessHistory.recordDataAccess(fingerprint, chunk.getColumnPath());
        touchedDataPage = true;
        continue;
      }
      if (chunk
          .getDictionaryPageRange()
          .filter(
              range ->
                  overlaps(range.lowerEndpoint(), range.upperEndpoint(), startOffset, endOffset))
          .isPresent()) {
        accessHistory.recordDictionaryAccess(fingerprint, chunk.getColumnPath());
        filterTracker.recordDictionaryRead(rowGroupOrdinal, chunk.getColumnPath());
      }
    }
    if (touchedDataPage) {
      onDataPageRead(rowGroupOrdinal);
    }
    return touchedDataPage;
  }

  /** Records a data read in {@code rowGroupOrdinal}. */
  private void onDataPageRead(int rowGroupOrdinal) {
    filterTracker.recordDataRead(rowGroupOrdinal);
  }

  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < otherEnd && end > otherStart;
  }

  /**
   * Prefetches dictionary pages across all row groups when the schema has filter columns, or
   * prefetches the first row group's data columns otherwise.
   */
  private void prefetchFirstRowGroupOnce(VectoredSeekableByteChannel source) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (!dictionaryColumns.isEmpty()) {
      speculateDictionaryPagesFrom(source, 0);
      if (layout.getRowGroups().size() == 1) {
        long ignored = speculateRowGroup(source, 0);
      }
      return;
    }
    long ignored = speculateRowGroup(source, 0);
  }

  /**
   * Prefetches dictionary page ranges for known dictionary columns starting at {@code startRg}, and
   * remembers those columns as prefetched once every range is registered in the buffer cache, until
   * one of those downloads fails.
   */
  private void speculateDictionaryPagesFrom(VectoredSeekableByteChannel source, int startRg) {
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (dictionaryColumns.isEmpty()) {
      return;
    }
    List<Range<Long>> dictRanges = new ArrayList<>();
    List<Range<Long>> unusedDataRanges = new ArrayList<>();
    int rowGroupCount = layout.getRowGroups().size();
    for (int ordinal = startRg; ordinal < rowGroupCount; ordinal++) {
      collectRowGroupRanges(
          dictRanges, unusedDataRanges, ordinal, ImmutableSet.of(), dictionaryColumns);
    }
    dictRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    List<Range<Long>> coalescedRanges = coalesceConnectedRanges(dictRanges);
    long ignored = scheduler.schedule(source, itemId, coalescedRanges, fileSize);
    if (areAllRangesRegistered(coalescedRanges)) {
      prefetchedDictionaryColumns = dictionaryColumns;
      forgetPrefetchedDictionariesOnFailure(coalescedRanges, dictionaryColumns);
    }
  }

  /**
   * Clears {@code prefetchedDictionaryColumns} if any of {@code ranges} fails to download, because
   * the ranges are usually still in flight when they are first found registered, and a failed range
   * is dropped from the buffer cache and must be scheduled again by a later dictionary read.
   */
  private void forgetPrefetchedDictionariesOnFailure(
      List<Range<Long>> ranges, ImmutableSet<String> dictionaryColumns) {
    for (Range<Long> range : ranges) {
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
      bufferCache
          .getRangeCovering(itemId, range.lowerEndpoint(), length)
          .ifPresent(
              cachedRange -> {
                CompletableFuture<ByteBuffer> unused =
                    cachedRange
                        .getFuture()
                        .whenComplete(
                            (buffer, error) -> {
                              if ((error != null || buffer == null)
                                  && prefetchedDictionaryColumns == dictionaryColumns) {
                                prefetchedDictionaryColumns = ImmutableSet.of();
                              }
                            });
              });
    }
  }

  /**
   * Returns whether every range is cached or in flight, which is false when the scheduler dropped
   * ranges beyond its concurrency budget or an earlier download already failed.
   */
  private boolean areAllRangesRegistered(List<Range<Long>> ranges) {
    for (Range<Long> range : ranges) {
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
      if (!bufferCache.getRangeCovering(itemId, range.lowerEndpoint(), length).isPresent()) {
        return false;
      }
    }
    return true;
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
    targetNextOrdinal.ifPresent(this::markDataPrefetchedIfQueued);
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
    long scheduledBytes = scheduler.schedule(source, itemId, ranges, fileSize);
    markDataPrefetchedIfQueued(rowGroupOrdinal);
    return scheduledBytes;
  }

  /**
   * Remembers {@code rowGroupOrdinal} as data-prefetched only when it has known data pages and all
   * of them are cached or in flight, so that a speculation that queued only dictionary pages, or
   * whose data ranges were dropped by the scheduler, still lets the dictionary trigger fetch them.
   */
  private void markDataPrefetchedIfQueued(int rowGroupOrdinal) {
    int fingerprint = layout.getSchemaFingerprint();
    List<Range<Long>> unusedDictRanges = new ArrayList<>();
    List<Range<Long>> dataRanges = new ArrayList<>();
    collectRowGroupRanges(
        unusedDictRanges,
        dataRanges,
        rowGroupOrdinal,
        accessHistory.getDataColumns(fingerprint),
        accessHistory.getDictionaryColumns(fingerprint));
    if (dataRanges.isEmpty()) {
      return;
    }
    dataRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    if (areAllRangesRegistered(coalesceConnectedRanges(dataRanges))) {
      rowGroupsWithDataPrefetch.add(rowGroupOrdinal);
    }
  }

  /**
   * Fetches the data pages of the known data columns in a single row group, skipping a column's
   * dictionary page only while that page is cached or in flight, and returns whether every data
   * page range is now cached or in flight.
   *
   * <p>A column whose dictionary page is not in the buffer cache is fetched from its chunk start,
   * because a later read of the whole chunk can only be served from one contiguous cached span.
   */
  private boolean speculateRowGroupDataPages(
      VectoredSeekableByteChannel source, int rowGroupOrdinal) {
    int fingerprint = layout.getSchemaFingerprint();
    List<Range<Long>> unusedDictRanges = new ArrayList<>();
    List<Range<Long>> dataRanges = new ArrayList<>();
    collectRowGroupRanges(
        unusedDictRanges,
        dataRanges,
        rowGroupOrdinal,
        accessHistory.getDataColumns(fingerprint),
        columnsWithRegisteredDictionaryPage(
            rowGroupOrdinal, accessHistory.getDictionaryColumns(fingerprint)));
    if (dataRanges.isEmpty()) {
      return false;
    }
    dataRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    List<Range<Long>> coalescedRanges = coalesceConnectedRanges(dataRanges);
    long ignored = scheduler.schedule(source, itemId, coalescedRanges, fileSize);
    return areAllRangesRegistered(coalescedRanges);
  }

  /**
   * Returns the columns of {@code dictionaryColumns} whose dictionary page in {@code
   * rowGroupOrdinal} is currently cached or in flight.
   */
  private ImmutableSet<String> columnsWithRegisteredDictionaryPage(
      int rowGroupOrdinal, Set<String> dictionaryColumns) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return ImmutableSet.of();
    }
    ImmutableSet.Builder<String> registered = ImmutableSet.builder();
    for (String columnPath : dictionaryColumns) {
      Optional<Range<Long>> dictionaryPage =
          rowGroup
              .get()
              .getColumnChunk(columnPath)
              .flatMap(ParquetColumnChunk::getDictionaryPageRange);
      if (dictionaryPage.isPresent()
          && areAllRangesRegistered(ImmutableList.of(dictionaryPage.get()))) {
        registered.add(columnPath);
      }
    }
    return registered.build();
  }

  /**
   * Returns the sorted exact byte ranges covering the columns known for this schema in the current
   * row group and, when present, the next surviving row group.
   */
  private List<Range<Long>> collectLearnedRangesForRowGroups(
      int currentRowGroupOrdinal, OptionalInt nextRowGroupOrdinal) {
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(fingerprint);
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    if (dataColumns.isEmpty() && dictionaryColumns.isEmpty()) {
      return new ArrayList<>();
    }
    List<Range<Long>> combined =
        new ArrayList<>(
            collectCoalescedRowGroupRanges(
                currentRowGroupOrdinal,
                dataColumns,
                Sets.intersection(dictionaryColumns, dataColumns)));
    if (nextRowGroupOrdinal.isPresent()) {
      combined.addAll(
          collectCoalescedRowGroupRanges(
              nextRowGroupOrdinal.getAsInt(), dataColumns, dictionaryColumns));
    }
    combined.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return combined;
  }

  private List<Range<Long>> collectCoalescedRowGroupRanges(
      int rowGroupOrdinal, Set<String> dataColumns, Set<String> dictionaryColumns) {
    List<Range<Long>> dictRanges = new ArrayList<>();
    List<Range<Long>> dataRanges = new ArrayList<>();
    collectRowGroupRanges(dictRanges, dataRanges, rowGroupOrdinal, dataColumns, dictionaryColumns);
    dictRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    dataRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    List<Range<Long>> combined = new ArrayList<>(dictRanges.size() + dataRanges.size());
    combined.addAll(coalesceConnectedRanges(dictRanges));
    combined.addAll(coalesceConnectedRanges(dataRanges));
    combined.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return combined;
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
      List<Range<Long>> dictRanges,
      List<Range<Long>> dataRanges,
      int rowGroupOrdinal,
      Set<String> dataColumns,
      Set<String> dictionaryColumns) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return;
    }
    for (String columnPath : dictionaryColumns) {
      rowGroup
          .get()
          .getColumnChunk(columnPath)
          .flatMap(ParquetColumnChunk::getDictionaryPageRange)
          .ifPresent(dictRanges::add);
    }
    for (String columnPath : dataColumns) {
      rowGroup
          .get()
          .getColumnChunk(columnPath)
          .ifPresent(
              chunk -> {
                long start =
                    dictionaryColumns.contains(columnPath)
                            && chunk.getDataPageOffset() > chunk.getStartOffset()
                            && chunk.getDataPageOffset() < chunk.getEndOffset()
                        ? chunk.getDataPageOffset()
                        : chunk.getStartOffset();
                if (chunk.getEndOffset() > start) {
                  dataRanges.add(Range.closedOpen(start, chunk.getEndOffset()));
                }
              });
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
