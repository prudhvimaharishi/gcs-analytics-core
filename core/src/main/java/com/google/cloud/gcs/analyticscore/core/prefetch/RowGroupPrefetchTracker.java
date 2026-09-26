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
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Tracks per-stream read progress across a {@link ParquetFileLayout}, records learned data and
 * dictionary columns into {@link SchemaAccessHistory}, and computes speculative byte ranges to
 * prefetch.
 */
final class RowGroupPrefetchTracker {

  private final ParquetFileLayout layout;
  private final SchemaAccessHistory accessHistory;
  private final long maxBlockSizeBytes;
  private final Set<Integer> prefetchedRowGroups = ConcurrentHashMap.newKeySet();
  private final Set<Integer> rowGroupsWithDictionaryRead = new HashSet<>();
  private ImmutableSet<String> prefetchedDictionaryColumns = ImmutableSet.of();
  private boolean firstRowGroupPrefetched;
  private int pendingSpeculationRowGroupIndex = -1;
  private int lastDataReadIndex = -1;
  @Nullable private Range<Long> lastObservedDataPageRange;

  RowGroupPrefetchTracker(
      ParquetFileLayout layout, SchemaAccessHistory accessHistory, long maxBlockSizeBytes) {
    this.layout = checkNotNull(layout, "layout cannot be null");
    this.accessHistory = checkNotNull(accessHistory, "accessHistory cannot be null");
    this.maxBlockSizeBytes = maxBlockSizeBytes;
  }

  /**
   * Returns whether {@code [position, position + length)} overlaps any column chunk's data pages.
   */
  boolean touchesDataPages(long position, int length) {
    return layout
        .findRowGroupAt(position)
        .map(rowGroup -> !rowGroup.findDataColumnsInRange(position, position + length).isEmpty())
        .orElse(false);
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
   * Records the columns touched by a vectored read and remembers the next row group to speculate
   * once the foreground read completes.
   */
  void onVectoredRead(List<GcsObjectRange> ranges) {
    int lastRowGroupIndex = -1;
    for (GcsObjectRange range : ranges) {
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
    pendingSpeculationRowGroupIndex = lastRowGroupIndex + 1;
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
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(fingerprint);
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    ImmutableList<Range<Long>> coalescedRanges =
        collectRowGroupRanges(
            rowGroupIndex, dataColumns, Sets.intersection(dataColumns, dictionaryColumns));
    if (!coalescedRanges.isEmpty() && scheduleRanges.test(coalescedRanges)) {
      prefetchedRowGroups.add(rowGroupIndex);
    }
  }

  private boolean recordColumnsInRange(ParquetRowGroup rowGroup, long startOffset, long endOffset) {
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> touchedDataColumns =
        rowGroup.findDataColumnsInRange(startOffset, endOffset);
    for (String columnPath : touchedDataColumns) {
      accessHistory.recordDataAccess(fingerprint, columnPath);
    }
    for (String columnPath :
        findDictionaryOnlyColumns(rowGroup, startOffset, endOffset, touchedDataColumns)) {
      accessHistory.recordDictionaryAccess(fingerprint, columnPath);
      if (rowGroup.getIndex() > lastDataReadIndex) {
        rowGroupsWithDictionaryRead.add(rowGroup.getIndex());
      }
    }
    if (!touchedDataColumns.isEmpty()) {
      lastDataReadIndex = Math.max(lastDataReadIndex, rowGroup.getIndex());
      return true;
    }
    return false;
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

  private void prefetchFirstRowGroupOnce(Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (!dictionaryColumns.isEmpty()) {
      speculateDictionaryPagesFrom(0, scheduleRanges);
      if (layout.getRowGroups().size() == 1) {
        prefetchRowGroup(0, scheduleRanges);
      }
      return;
    }
    prefetchRowGroup(0, scheduleRanges);
  }

  private void onDictionaryPageRead(
      ParquetRowGroup rowGroup, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    int rowGroupIndex = rowGroup.getIndex();
    ImmutableSet<String> dictionaryColumns =
        accessHistory.getDictionaryColumns(layout.getSchemaFingerprint());
    if (!prefetchedDictionaryColumns.containsAll(dictionaryColumns)) {
      speculateDictionaryPagesFrom(rowGroupIndex + 1, scheduleRanges);
    }
    if (!prefetchedRowGroups.contains(rowGroupIndex)
        && rowGroupsWithDictionaryRead.contains(rowGroupIndex)
        && !hasPendingDataPrefetchBefore(rowGroupIndex)) {
      prefetchRowGroup(rowGroupIndex, scheduleRanges);
    }
  }

  private boolean hasPendingDataPrefetchBefore(int rowGroupIndex) {
    for (int prefetchedIndex : prefetchedRowGroups) {
      if (prefetchedIndex > lastDataReadIndex && prefetchedIndex < rowGroupIndex) {
        return true;
      }
    }
    return false;
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
      dictionaryRanges.addAll(collectRowGroupRanges(index, ImmutableSet.of(), dictionaryColumns));
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
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(fingerprint);
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    List<Range<Long>> ranges =
        new ArrayList<>(
            collectRowGroupRanges(
                rowGroupIndex, dataColumns, Sets.intersection(dataColumns, dictionaryColumns)));
    ranges.removeIf(range -> range.lowerEndpoint() < currentReadEnd);
    if (!ranges.isEmpty()) {
      boolean unused = scheduleRanges.test(ImmutableList.copyOf(ranges));
    }
    int nextIndex = rowGroupIndex + 1;
    if (shouldSpeculateNextRowGroup(nextIndex)) {
      prefetchRowGroup(nextIndex, scheduleRanges);
    }
  }

  private ImmutableList<Range<Long>> collectRowGroupRanges(
      int rowGroupIndex, Set<String> dataColumns, Set<String> dictionaryColumns) {
    return layout
        .getRowGroup(rowGroupIndex)
        .map(
            rowGroup ->
                rowGroup.getCoalescedColumnRanges(
                    dataColumns, dictionaryColumns, maxBlockSizeBytes))
        .orElse(ImmutableList.of());
  }

  private boolean shouldSpeculateNextRowGroup(int targetIndex) {
    return targetIndex >= 0 && targetIndex < layout.getRowGroups().size();
  }

  private boolean isWithinLastDataPageRange(long position, int length) {
    return lastObservedDataPageRange != null
        && position >= lastObservedDataPageRange.lowerEndpoint()
        && position + length <= lastObservedDataPageRange.upperEndpoint();
  }
}
