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

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.DictionaryTrigger;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Tracks per-stream read progress across a {@link ParquetFileLayout}, records learned data and
 * dictionary columns into {@link SchemaAccessHistory}, and computes speculative byte ranges to
 * prefetch.
 */
final class RowGroupPrefetchTracker {

  private static final long SPLIT_BOUNDARY_ROW_GROUP_BYTES = 64L * 1024 * 1024;

  private final ParquetFileLayout layout;
  private final SchemaAccessHistory accessHistory;
  private final long maxBlockSizeBytes;
  private final DictionaryTrigger dictionaryTrigger;
  private final BiConsumer<Long, Long> cancelRangeWindow;
  private final RowGroupFilterTracker filterTracker = new RowGroupFilterTracker();
  private final Set<Integer> prefetchedRowGroups = ConcurrentHashMap.newKeySet();
  private ImmutableSet<String> prefetchedDictionaryColumns = ImmutableSet.of();
  private boolean firstRowGroupPrefetched;
  private boolean outcomeRecorded;
  private int pendingSpeculationRowGroupIndex = -1;
  @Nullable private Range<Long> lastObservedDataPageRange;

  RowGroupPrefetchTracker(
      ParquetFileLayout layout,
      SchemaAccessHistory accessHistory,
      long maxBlockSizeBytes,
      DictionaryTrigger dictionaryTrigger,
      BiConsumer<Long, Long> cancelRangeWindow) {
    this.layout = checkNotNull(layout, "layout cannot be null");
    this.accessHistory = checkNotNull(accessHistory, "accessHistory cannot be null");
    this.maxBlockSizeBytes = maxBlockSizeBytes;
    this.dictionaryTrigger = checkNotNull(dictionaryTrigger, "dictionaryTrigger cannot be null");
    this.cancelRangeWindow = checkNotNull(cancelRangeWindow, "cancelRangeWindow cannot be null");
  }

  /**
   * Observes a read at {@code [position, position + length)} before blocking on dictionary bytes.
   * If the read touches only dictionary pages, records the dictionary access and schedules any
   * triggered speculative ranges immediately.
   *
   * @return {@code true} if the read was a dictionary-only read and was handled
   */
  boolean onBeforeRead(
      long position, int length, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (length <= 0) {
      return false;
    }
    Optional<ParquetRowGroup> maybeRowGroup = layout.findRowGroupAt(position);
    if (!maybeRowGroup.isPresent()) {
      return false;
    }
    ParquetRowGroup rowGroup = maybeRowGroup.get();
    long endOffset = position + length;
    if (!rowGroup.findDataColumnsInRange(position, endOffset).isEmpty()) {
      return false;
    }
    ImmutableSet<String> dictionaryColumns =
        rowGroup.findDictionaryColumnsInRange(position, endOffset);
    if (dictionaryColumns.isEmpty()) {
      return false;
    }
    firstRowGroupPrefetched = true;
    lastObservedDataPageRange = null;
    recordDictionaryColumns(rowGroup.getIndex(), dictionaryColumns);
    onDictionaryPageRead(rowGroup, scheduleRanges);
    return true;
  }

  /**
   * Observes a single-buffer read at {@code [position, position + length)} and invokes {@code
   * scheduleRanges} with any speculative byte ranges triggered by the read.
   */
  void onSingleRead(
      long position, int length, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (isWithinLastDataPageRange(position, length)) {
      return;
    }
    Optional<ParquetRowGroup> maybeRowGroup = layout.findRowGroupAt(position);
    if (!maybeRowGroup.isPresent()) {
      prefetchFirstRowGroupOnce(scheduleRanges);
      return;
    }
    ParquetRowGroup rowGroup = maybeRowGroup.get();
    firstRowGroupPrefetched = true;
    long endOffset = position + length;
    if (recordColumnsInRange(rowGroup, position, endOffset)) {
      speculateRowGroups(rowGroup.getIndex(), endOffset, scheduleRanges);
    } else {
      onDictionaryPageRead(rowGroup, scheduleRanges);
    }
    lastObservedDataPageRange = rowGroup.findDataPageRangeAt(position).orElse(null);
  }

  /**
   * Records the columns touched by a vectored read in file order, so that a later row group in the
   * same call cannot mark an earlier one as skipped, and remembers the next surviving row group to
   * speculate once the foreground read completes.
   */
  void onVectoredRead(List<GcsObjectRange> ranges) {
    int lastRowGroupIndex = -1;
    List<GcsObjectRange> rangesInFileOrder = new ArrayList<>(ranges);
    rangesInFileOrder.sort(Comparator.comparingLong(GcsObjectRange::getOffset));
    for (GcsObjectRange range : rangesInFileOrder) {
      Optional<ParquetRowGroup> maybeRowGroup = layout.findRowGroupAt(range.getOffset());
      if (maybeRowGroup.isPresent()
          && recordColumnsInRange(
              maybeRowGroup.get(), range.getOffset(), range.getOffset() + range.getLength())) {
        lastRowGroupIndex = Math.max(lastRowGroupIndex, maybeRowGroup.get().getIndex());
      }
    }
    if (lastRowGroupIndex < 0) {
      return;
    }
    firstRowGroupPrefetched = true;
    pendingSpeculationRowGroupIndex =
        filterTracker
            .findNextSurvivingRowGroup(
                layout,
                lastRowGroupIndex,
                accessHistory.getDictionaryColumns(layout.getSchemaFingerprint()))
            .orElse(-1);
  }

  /**
   * Consumes the pending next-row-group index recorded by {@link #onVectoredRead(List)}, or
   * triggers first-row-group prefetching if the vectored read did not touch any data pages.
   */
  OptionalInt pollPendingNextRowGroup(Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (pendingSpeculationRowGroupIndex < 0) {
      prefetchFirstRowGroupOnce(scheduleRanges);
      return OptionalInt.empty();
    }
    firstRowGroupPrefetched = true;
    int targetIndex = pendingSpeculationRowGroupIndex;
    pendingSpeculationRowGroupIndex = -1;
    return shouldSpeculateNextRowGroup(targetIndex)
        ? OptionalInt.of(targetIndex)
        : OptionalInt.empty();
  }

  /** Schedules the learned data columns of {@code rowGroupIndex}. */
  void prefetchRowGroup(int rowGroupIndex, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (prefetchedRowGroups.contains(rowGroupIndex)) {
      return;
    }
    ImmutableList<Range<Long>> coalescedRanges = collectDataRanges(rowGroupIndex);
    if (!coalescedRanges.isEmpty() && scheduleRanges.test(coalescedRanges)) {
      prefetchedRowGroups.add(rowGroupIndex);
    }
  }

  /**
   * Records how the file was rejected if the stream closes without reading any data page, for the
   * read-rate gates of the schema.
   */
  void recordOutcomeOnClose() {
    if (filterTracker.getDictionaryTouchedCount() > 0) {
      recordOutcomeOnce(SchemaAccessHistory.FileFilterOutcome.DICT_REJECTED);
    } else {
      recordOutcomeOnce(SchemaAccessHistory.FileFilterOutcome.FOOTER_REJECTED);
    }
  }

  private void recordOutcomeOnce(SchemaAccessHistory.FileFilterOutcome outcome) {
    if (outcomeRecorded) {
      return;
    }
    outcomeRecorded = true;
    accessHistory.recordFileOutcome(layout.getSchemaFingerprint(), outcome);
  }

  private boolean recordColumnsInRange(ParquetRowGroup rowGroup, long startOffset, long endOffset) {
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> touchedDataColumns =
        rowGroup.findDataColumnsInRange(startOffset, endOffset);
    for (String columnPath : touchedDataColumns) {
      accessHistory.recordDataAccess(fingerprint, columnPath);
    }
    recordDictionaryColumns(
        rowGroup.getIndex(),
        findDictionaryOnlyColumns(rowGroup, startOffset, endOffset, touchedDataColumns));
    if (!touchedDataColumns.isEmpty()) {
      onDataPageRead(rowGroup.getIndex());
      return true;
    }
    return false;
  }

  private void recordDictionaryColumns(int rowGroupIndex, Set<String> columnPaths) {
    int fingerprint = layout.getSchemaFingerprint();
    for (String columnPath : columnPaths) {
      accessHistory.recordDictionaryAccess(fingerprint, columnPath);
      filterTracker.recordDictionaryRead(rowGroupIndex, columnPath);
    }
  }

  /**
   * Returns the columns whose dictionary page is touched by {@code [startOffset, endOffset)} but
   * whose data pages are not, because readers read a column chunk starting at its dictionary page.
   */
  private static Set<String> findDictionaryOnlyColumns(
      ParquetRowGroup rowGroup,
      long startOffset,
      long endOffset,
      ImmutableSet<String> touchedDataColumns) {
    return Sets.difference(
        rowGroup.findDictionaryColumnsInRange(startOffset, endOffset), touchedDataColumns);
  }

  private void onDataPageRead(int rowGroupIndex) {
    recordOutcomeOnce(SchemaAccessHistory.FileFilterOutcome.SURVIVED);
    int cleanupStartIndex = Math.max(0, filterTracker.getLastDataReadIndex());
    filterTracker.recordDataRead(rowGroupIndex);
    if (rowGroupIndex > cleanupStartIndex) {
      cancelRangeWindow.accept(
          layout.getRowGroups().get(cleanupStartIndex).getStartOffset(),
          layout.getRowGroups().get(rowGroupIndex).getStartOffset());
    }
  }

  private void prefetchFirstRowGroupOnce(Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    if (accessHistory.shouldSpeculateAtFooter(layout.getSchemaFingerprint())) {
      speculateDictionaryPagesFrom(0, scheduleRanges);
    }
  }

  private void onDictionaryPageRead(
      ParquetRowGroup rowGroup, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    int rowGroupIndex = rowGroup.getIndex();
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    if (!prefetchedDictionaryColumns.containsAll(dictionaryColumns)) {
      boolean deferredAtFooter = !accessHistory.shouldSpeculateAtFooter(fingerprint);
      speculateDictionaryPagesFrom(
          deferredAtFooter ? rowGroupIndex : rowGroupIndex + 1, scheduleRanges);
    }
    if (!prefetchedRowGroups.contains(rowGroupIndex)
        && accessHistory.shouldSpeculateOnDictionary(fingerprint)
        && isDictionaryTriggerMet(rowGroupIndex, dictionaryColumns)
        && !hasPendingDataPrefetchBefore(rowGroupIndex)) {
      prefetchRowGroup(rowGroupIndex, scheduleRanges);
    }
  }

  private boolean hasPendingDataPrefetchBefore(int rowGroupIndex) {
    int lastDataReadIndex = filterTracker.getLastDataReadIndex();
    for (int prefetchedIndex : prefetchedRowGroups) {
      if (prefetchedIndex > lastDataReadIndex && prefetchedIndex < rowGroupIndex) {
        return true;
      }
    }
    return false;
  }

  private boolean isDictionaryTriggerMet(int rowGroupIndex, Set<String> dictionaryColumns) {
    switch (dictionaryTrigger) {
      case FIRST_DICT_READ:
        return filterTracker.hasReadAnyDictionary(rowGroupIndex, dictionaryColumns);
      case LAST_DICT_READ:
        return filterTracker.hasReadAllDictionaries(layout, rowGroupIndex, dictionaryColumns);
    }
    throw new IllegalStateException("Unknown dictionary trigger: " + dictionaryTrigger);
  }

  private void speculateDictionaryPagesFrom(
      int startRowGroupIndex, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (dictionaryColumns.isEmpty()) {
      return;
    }
    ImmutableList.Builder<Range<Long>> dictionaryRanges = ImmutableList.builder();
    int rowGroupCount = layout.getRowGroups().size();
    for (int index = startRowGroupIndex; index < rowGroupCount; index++) {
      Set<String> unreadColumns =
          Sets.difference(dictionaryColumns, filterTracker.getDictionaryColumnsRead(index));
      layout
          .getRowGroup(index)
          .ifPresent(
              rowGroup -> dictionaryRanges.addAll(rowGroup.getDictionaryPageRanges(unreadColumns)));
    }
    ImmutableList<Range<Long>> ranges = dictionaryRanges.build();
    if (ranges.isEmpty() || scheduleRanges.test(ranges)) {
      prefetchedDictionaryColumns = dictionaryColumns;
    }
  }

  private void speculateRowGroups(
      int rowGroupIndex,
      long currentReadEnd,
      Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    List<Range<Long>> ranges = new ArrayList<>(collectDataRanges(rowGroupIndex));
    ranges.removeIf(range -> range.lowerEndpoint() < currentReadEnd);
    if (!ranges.isEmpty()) {
      boolean unused = scheduleRanges.test(ImmutableList.copyOf(ranges));
    }
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    OptionalInt nextIndex =
        filterTracker.findNextSurvivingRowGroup(layout, rowGroupIndex, dictionaryColumns);
    if (nextIndex.isPresent() && shouldSpeculateNextRowGroup(nextIndex.getAsInt())) {
      prefetchRowGroup(nextIndex.getAsInt(), scheduleRanges);
    }
  }

  /**
   * Returns the merged whole column chunks of the learned data columns in {@code rowGroupIndex}.
   */
  private ImmutableList<Range<Long>> collectDataRanges(int rowGroupIndex) {
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(layout.getSchemaFingerprint());
    return layout
        .getRowGroup(rowGroupIndex)
        .map(rowGroup -> rowGroup.getCoalescedColumnRanges(dataColumns, maxBlockSizeBytes))
        .orElse(ImmutableList.of());
  }

  /**
   * Returns whether {@code targetIndex} may be prefetched as the next row group. A row group whose
   * dictionary pages this stream has read was visited by the engine and so belongs to this task's
   * split; otherwise the row groups of a split-sized file are assumed to belong to other tasks.
   */
  private boolean shouldSpeculateNextRowGroup(int targetIndex) {
    if (targetIndex < 0 || targetIndex >= layout.getRowGroups().size()) {
      return false;
    }
    return !filterTracker.getDictionaryColumnsRead(targetIndex).isEmpty()
        || !isSplitMultiRowGroupFile();
  }

  private boolean isSplitMultiRowGroupFile() {
    List<ParquetRowGroup> rowGroups = layout.getRowGroups();
    return rowGroups.size() > 1
        && rowGroups.get(0).getEndOffset() - rowGroups.get(0).getStartOffset()
            >= SPLIT_BOUNDARY_ROW_GROUP_BYTES;
  }

  private boolean isWithinLastDataPageRange(long position, int length) {
    return lastObservedDataPageRange != null
        && position >= lastObservedDataPageRange.lowerEndpoint()
        && position + length <= lastObservedDataPageRange.upperEndpoint();
  }
}
