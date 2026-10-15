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
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  private boolean firstRowGroupPrefetched;
  @Nullable private Range<Long> lastObservedDataPageRange;

  RowGroupPrefetchTracker(ParquetFileLayout layout, SchemaAccessHistory accessHistory) {
    this.layout = checkNotNull(layout, "layout cannot be null");
    this.accessHistory = checkNotNull(accessHistory, "accessHistory cannot be null");
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
    recordColumnsInRange(rowGroup, position, position + length);
    lastObservedDataPageRange = rowGroup.findDataPageRangeAt(position).orElse(null);
  }

  /** Records the columns touched by a vectored read. */
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
  }

  /** Schedules the learned data columns of {@code rowGroupIndex}. */
  private void prefetchRowGroup(
      int rowGroupIndex, Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    boolean unused = scheduleRanges.test(collectDataRanges(rowGroupIndex));
  }

  private boolean recordColumnsInRange(ParquetRowGroup rowGroup, long startOffset, long endOffset) {
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> touchedDataColumns =
        rowGroup.findDataColumnsInRange(startOffset, endOffset);
    for (String columnPath : touchedDataColumns) {
      accessHistory.recordDataAccess(fingerprint, columnPath);
    }
    recordDictionaryColumns(
        findDictionaryOnlyColumns(rowGroup, startOffset, endOffset, touchedDataColumns));
    return !touchedDataColumns.isEmpty();
  }

  private void recordDictionaryColumns(Set<String> columnPaths) {
    int fingerprint = layout.getSchemaFingerprint();
    for (String columnPath : columnPaths) {
      accessHistory.recordDictionaryAccess(fingerprint, columnPath);
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

  private void prefetchFirstRowGroupOnce(Predicate<ImmutableList<Range<Long>>> scheduleRanges) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    if (isUnfilteredSingleRowGroupFile()) {
      prefetchRowGroup(0, scheduleRanges);
    }
  }

  /**
   * Returns whether the file has a single row group and no dictionary columns are learned, so the
   * reader goes straight from the footer to that row group's data.
   */
  private boolean isUnfilteredSingleRowGroupFile() {
    return layout.getRowGroups().size() == 1
        && accessHistory.getDictionaryColumns(layout.getSchemaFingerprint()).isEmpty();
  }

  /**
   * Returns the merged whole column chunks of the learned data columns in {@code rowGroupIndex}.
   */
  private ImmutableList<Range<Long>> collectDataRanges(int rowGroupIndex) {
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(layout.getSchemaFingerprint());
    return layout
        .getRowGroup(rowGroupIndex)
        .map(rowGroup -> rowGroup.getCoalescedColumnRanges(dataColumns))
        .orElse(ImmutableList.of());
  }

  private boolean isWithinLastDataPageRange(long position, int length) {
    return lastObservedDataPageRange != null
        && position >= lastObservedDataPageRange.lowerEndpoint()
        && position + length <= lastObservedDataPageRange.upperEndpoint();
  }
}
