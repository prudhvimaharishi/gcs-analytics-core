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
 * A {@link FormatOptimizer} that speculatively fetches exact Parquet column chunks and dictionary
 * pages before the engine asks for them.
 *
 * <p>Footer I/O is owned exclusively by {@code GcsFooterOptimizer}; this optimizer consumes the
 * footer published in {@link AnalyticsCacheManager}. If no footer is present in the global cache,
 * prefetching is skipped.
 *
 * <p>Speculation schedules exact column chunk and dictionary page byte ranges rather than fixed
 * blocks, avoiding read amplification on unprojected columns while allowing the underlying channel
 * to coalesce nearby ranges.
 *
 * <p>Instances are bound to a single stream and are not thread-safe.
 */
public final class PredictivePrefetchOptimizer implements FormatOptimizer {

  private static final String PARQUET_EXTENSION = ".parquet";
  private static final int MAX_CONCURRENT_PREFETCH_RANGES = 32;
  private static final long SPLIT_BOUNDARY_ROW_GROUP_BYTES = 64L * 1024 * 1024;

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
  private boolean splitRowGroupDataPrefetched;
  private boolean outcomeRecorded;
  @Nullable private ParquetFileLayout layout;
  private long[] rowGroupStartOffsets = new long[0];
  private long[] rowGroupEndOffsets = new long[0];
  private int currentRowGroupOrdinal = -1;
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
    int requestedLength = dst.remaining();
    int servedBytes = serveFromCache(position, dst);

    ensureLayoutLoaded();
    if (layout != null) {
      observeAccess(source, position, requestedLength);
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
    if (targetOrdinal < 0 || targetOrdinal >= rowGroupStartOffsets.length) {
      return false;
    }
    return !isSplitMultiRowGroupFile() || filterTracker.getDataTouchedCount() >= 2;
  }

  @Override
  public void onClose() {
    closed = true;
    recordOutcomeOnClose();
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private void recordOutcomeOnClose() {
    if (outcomeRecorded || layout == null || accessHistory == null) {
      return;
    }
    outcomeRecorded = true;
    int fingerprint = layout.getSchemaFingerprint();
    if (filterTracker.getDataTouchedCount() > 0) {
      accessHistory.recordFileOutcome(fingerprint, SchemaAccessHistory.FileFilterOutcome.SURVIVED);
    } else if (filterTracker.getDictionaryTouchedCount() > 0) {
      accessHistory.recordFileOutcome(
          fingerprint, SchemaAccessHistory.FileFilterOutcome.DICT_REJECTED);
    } else {
      accessHistory.recordFileOutcome(
          fingerprint, SchemaAccessHistory.FileFilterOutcome.FOOTER_REJECTED);
    }
  }

  private int serveFromCache(long position, ByteBuffer dst) {
    int servedBytes = bufferCache.copyInto(itemId, position, dst);
    if (servedBytes == 0) {
      telemetry.recordMetric(Metric.PREFETCH_CACHE_MISS, 1L, Collections.emptyMap());
      return 0;
    }
    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    telemetry.recordMetric(Metric.PREFETCH_BYTES_CONSUMED, servedBytes, Collections.emptyMap());
    return servedBytes;
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
    if (rowGroupOrdinal != currentRowGroupOrdinal) {
      if (currentRowGroupOrdinal >= 0
          && rowGroupOrdinal > currentRowGroupOrdinal
          && !filterTracker.hasDataRead(currentRowGroupOrdinal)) {
        scheduler.cancelRangeWindow(
            rowGroupStartOffsets[currentRowGroupOrdinal],
            rowGroupEndOffsets[currentRowGroupOrdinal]);
      }
      currentRowGroupOrdinal = rowGroupOrdinal;
    }
    long currentReadEnd = position + requestedLength;
    if (recordColumnsInRange(rowGroupOrdinal, position, currentReadEnd)) {
      speculateRowGroups(source, rowGroupOrdinal, currentReadEnd);
    } else {
      int fingerprint = layout.getSchemaFingerprint();
      boolean deferredAtFooter = !accessHistory.shouldSpeculateAtFooter(fingerprint);
      speculateDictionaryPagesFrom(
          source, deferredAtFooter ? rowGroupOrdinal : rowGroupOrdinal + 1);
      if (!splitRowGroupDataPrefetched
          && (isSplitMultiRowGroupFile() || deferredAtFooter)
          && accessHistory.shouldSpeculateOnDictionary(fingerprint)
          && filterTracker.hasReadAllDictionaries(
              rowGroupOrdinal, accessHistory.getDictionaryColumns(fingerprint))) {
        splitRowGroupDataPrefetched = true;
        long ignored = speculateRowGroup(source, rowGroupOrdinal);
      }
    }
  }

  private boolean isSplitMultiRowGroupFile() {
    return rowGroupStartOffsets.length > 1
        && (rowGroupEndOffsets[0] - rowGroupStartOffsets[0]) >= SPLIT_BOUNDARY_ROW_GROUP_BYTES;
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
    ImmutableSet<String> knownDictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    pendingSpeculationRowGroupOrdinal =
        filterTracker
            .findNextSurvivingRowGroup(layout, lastRowGroupOrdinal, knownDictionaryColumns)
            .orElse(-1);
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
      if (!outcomeRecorded) {
        outcomeRecorded = true;
        accessHistory.recordFileOutcome(
            fingerprint, SchemaAccessHistory.FileFilterOutcome.SURVIVED);
      }
      filterTracker.recordDataRead(
          layout, rowGroupOrdinal, accessHistory.getDictionaryColumns(fingerprint));
    }
    return touchedDataPage;
  }

  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < otherEnd && end > otherStart;
  }

  /**
   * Prefetches dictionary pages across all row groups when the schema has filter columns, or
   * prefetches the first row group's data columns when the file is not split across multiple
   * split-sized row groups.
   */
  private void prefetchFirstRowGroupOnce(VectoredSeekableByteChannel source) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    int fingerprint = layout.getSchemaFingerprint();
    if (!accessHistory.shouldSpeculateAtFooter(fingerprint)) {
      return;
    }
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    if (!dictionaryColumns.isEmpty()) {
      speculateDictionaryPagesFrom(source, 0);
      if (layout.getRowGroups().size() == 1
          && accessHistory.shouldSpeculateOnDictionary(fingerprint)) {
        long ignored = speculateRowGroup(source, 0);
      }
      return;
    }
    if (isSplitMultiRowGroupFile()) {
      return;
    }
    long ignored = speculateRowGroup(source, 0);
  }

  /** Prefetches dictionary page ranges for known dictionary columns starting at {@code startRg}. */
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
    if (dictRanges.isEmpty()) {
      return;
    }
    dictRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    long ignored =
        scheduler.schedule(source, itemId, coalesceConnectedRanges(dictRanges), fileSize);
  }

  /**
   * Fetches the columns already known for this schema in the current row group (beyond {@code
   * currentReadEnd}) and the next surviving row group.
   */
  private void speculateRowGroups(
      VectoredSeekableByteChannel source, int rowGroupOrdinal, long currentReadEnd) {
    OptionalInt nextSurvivingOrdinal =
        filterTracker.findNextSurvivingRowGroup(
            layout,
            rowGroupOrdinal,
            accessHistory.getDictionaryColumns(layout.getSchemaFingerprint()));
    OptionalInt targetNextOrdinal =
        nextSurvivingOrdinal.isPresent()
                && shouldSpeculateNextRowGroup(nextSurvivingOrdinal.getAsInt())
            ? nextSurvivingOrdinal
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
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(fingerprint);
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    if (dataColumns.isEmpty() && dictionaryColumns.isEmpty()) {
      return new ArrayList<>();
    }
    List<Range<Long>> dictRanges = new ArrayList<>();
    List<Range<Long>> dataRanges = new ArrayList<>();
    collectRowGroupRanges(
        dictRanges, dataRanges, currentRowGroupOrdinal, dataColumns, dictionaryColumns);
    if (nextRowGroupOrdinal.isPresent()) {
      collectRowGroupRanges(
          dictRanges, dataRanges, nextRowGroupOrdinal.getAsInt(), dataColumns, dictionaryColumns);
    }
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
