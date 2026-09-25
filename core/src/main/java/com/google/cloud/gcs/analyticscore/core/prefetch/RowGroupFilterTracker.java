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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Tracks row-group filter outcomes within a single Parquet stream to predict which upcoming row
 * group will actually be read by the query engine.
 *
 * <p>Prediction relies on two heuristic signals observable within the current file:
 *
 * <ol>
 *   <li><b>Upfront Dictionary Sweep</b>: Engines such as Apache Iceberg evaluate footer min/max
 *       statistics across all row groups at file open and immediately read the dictionary pages of
 *       surviving candidate row groups. A row group skipped during this sweep, whose filter columns
 *       all have dictionary pages, is predicted to have failed the min/max check.
 *   <li><b>Rejected Min/Max Range Containment</b>: Whenever the engine skips data pages for a row
 *       group, that row group's filter-column {@code [min, max]} range is recorded as rejected. Any
 *       subsequent row group whose filter-column {@code [min, max]} falls completely within a
 *       rejected range is predicted to be filtered out as well.
 * </ol>
 *
 * <p>Both signals can be wrong, for example when an earlier row group was rejected by its
 * dictionary page contents rather than its min/max statistics, or by a second filter column this
 * tracker does not know about. A wrong prediction only costs a missed prefetch; it never changes
 * what the engine reads.
 */
final class RowGroupFilterTracker {

  private final SortedSet<Integer> dictionaryTouchedOrdinals = new TreeSet<>();
  private final SortedSet<Integer> dataTouchedOrdinals = new TreeSet<>();
  private final Map<Integer, Set<String>> dictionaryColumnsByRowGroup = new HashMap<>();
  private final Set<Integer> processedSkippedOrdinals = new HashSet<>();
  private final Set<String> filterColumnPaths = new HashSet<>();
  private final Map<String, List<ParquetColumnStatistics>> rejectedRangesByColumn = new HashMap<>();

  /** Records that a dictionary page in {@code rowGroupOrdinal} was read for {@code columnPath}. */
  void recordDictionaryRead(int rowGroupOrdinal, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    dictionaryTouchedOrdinals.add(rowGroupOrdinal);
    dictionaryColumnsByRowGroup
        .computeIfAbsent(rowGroupOrdinal, unused -> new HashSet<>())
        .add(columnPath);
    filterColumnPaths.add(columnPath);
  }

  /**
   * Returns whether all {@code knownDictionaryColumns} have been read in {@code rowGroupOrdinal}.
   */
  boolean hasReadAllDictionaries(int rowGroupOrdinal, Set<String> knownDictionaryColumns) {
    if (knownDictionaryColumns.isEmpty()) {
      return false;
    }
    Set<String> readColumns = dictionaryColumnsByRowGroup.get(rowGroupOrdinal);
    return readColumns != null && readColumns.containsAll(knownDictionaryColumns);
  }

  /**
   * Returns whether any of {@code knownDictionaryColumns} has been read in {@code rowGroupOrdinal}.
   */
  boolean hasReadAnyDictionary(int rowGroupOrdinal, Set<String> knownDictionaryColumns) {
    Set<String> readColumns = dictionaryColumnsByRowGroup.get(rowGroupOrdinal);
    return readColumns != null && !Collections.disjoint(readColumns, knownDictionaryColumns);
  }

  /** Returns the highest row group ordinal whose data pages have been read, or {@code -1}. */
  int getLastDataReadOrdinal() {
    return dataTouchedOrdinals.isEmpty() ? -1 : dataTouchedOrdinals.last();
  }

  /**
   * Records that data pages in {@code rowGroupOrdinal} were read, marking any earlier untouched row
   * groups as skipped and capturing their filter-column statistics as rejected.
   */
  void recordDataRead(
      ParquetFileLayout layout, int rowGroupOrdinal, Set<String> knownDictionaryColumns) {
    checkNotNull(layout, "layout cannot be null");
    checkNotNull(knownDictionaryColumns, "knownDictionaryColumns cannot be null");
    dataTouchedOrdinals.add(rowGroupOrdinal);
    markSkippedRowGroupsBefore(layout, rowGroupOrdinal, knownDictionaryColumns);
  }

  /**
   * Returns the next row group ordinal strictly after {@code afterOrdinal} that has not been
   * eliminated by upfront footer pruning or rejected min/max statistics, or empty if no remaining
   * row group survives.
   */
  OptionalInt findNextSurvivingRowGroup(
      ParquetFileLayout layout, int afterOrdinal, Set<String> knownDictionaryColumns) {
    checkNotNull(layout, "layout cannot be null");
    checkNotNull(knownDictionaryColumns, "knownDictionaryColumns cannot be null");

    int rowGroupCount = layout.getRowGroups().size();
    int maxDictOrdinal =
        dictionaryTouchedOrdinals.isEmpty() ? -1 : dictionaryTouchedOrdinals.last();

    for (int candidate = afterOrdinal + 1; candidate < rowGroupCount; candidate++) {
      if (isSkippedByUpfrontDictionarySweep(layout, candidate, afterOrdinal, maxDictOrdinal)) {
        continue;
      }
      if (isRejectedByStatistics(layout, candidate)) {
        continue;
      }
      return OptionalInt.of(candidate);
    }
    return OptionalInt.empty();
  }

  /**
   * Returns whether the engine's dictionary sweep passed over {@code candidate}. A row group where
   * any filter column has no dictionary page is never treated as skipped, because the engine could
   * not have read a dictionary there and may still read its data pages.
   */
  private boolean isSkippedByUpfrontDictionarySweep(
      ParquetFileLayout layout, int candidate, int afterOrdinal, int maxDictOrdinal) {
    return maxDictOrdinal > afterOrdinal
        && candidate <= maxDictOrdinal
        && !dictionaryTouchedOrdinals.contains(candidate)
        && allFilterColumnsHaveDictionaryPage(layout, candidate);
  }

  private boolean allFilterColumnsHaveDictionaryPage(ParquetFileLayout layout, int ordinal) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(ordinal);
    if (!rowGroup.isPresent()) {
      return false;
    }
    for (String columnPath : filterColumnPaths) {
      if (!rowGroup
          .get()
          .getColumnChunk(columnPath)
          .map(ParquetColumnChunk::hasDictionaryPage)
          .orElse(false)) {
        return false;
      }
    }
    return true;
  }

  private boolean isRejectedByStatistics(ParquetFileLayout layout, int candidateOrdinal) {
    if (rejectedRangesByColumn.isEmpty()) {
      return false;
    }
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(candidateOrdinal);
    if (!rowGroup.isPresent()) {
      return false;
    }
    for (Map.Entry<String, List<ParquetColumnStatistics>> entry :
        rejectedRangesByColumn.entrySet()) {
      String columnPath = entry.getKey();
      List<ParquetColumnStatistics> rejectedRanges = entry.getValue();
      Optional<ParquetColumnStatistics> candidateStats =
          rowGroup.get().getColumnChunk(columnPath).flatMap(ParquetColumnChunk::getStatistics);
      if (candidateStats.isPresent() && isContainedInAny(candidateStats.get(), rejectedRanges)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isContainedInAny(
      ParquetColumnStatistics candidate, List<ParquetColumnStatistics> rejectedRanges) {
    for (ParquetColumnStatistics rejected : rejectedRanges) {
      if (candidate.isContainedIn(rejected)) {
        return true;
      }
    }
    return false;
  }

  private void markSkippedRowGroupsBefore(
      ParquetFileLayout layout, int currentDataOrdinal, Set<String> knownDictionaryColumns) {
    Set<String> activeFilterColumns = new HashSet<>(filterColumnPaths);
    activeFilterColumns.addAll(knownDictionaryColumns);
    if (activeFilterColumns.isEmpty()) {
      return;
    }

    for (int ordinal = 0; ordinal < currentDataOrdinal; ordinal++) {
      if (dataTouchedOrdinals.contains(ordinal) || !processedSkippedOrdinals.add(ordinal)) {
        continue;
      }
      recordRejectedStatisticsForRowGroup(layout, ordinal, activeFilterColumns);
    }
  }

  private void recordRejectedStatisticsForRowGroup(
      ParquetFileLayout layout, int skippedOrdinal, Set<String> activeFilterColumns) {
    Optional<ParquetRowGroup> skippedRowGroup = layout.getRowGroup(skippedOrdinal);
    if (!skippedRowGroup.isPresent()) {
      return;
    }
    for (String columnPath : activeFilterColumns) {
      skippedRowGroup
          .get()
          .getColumnChunk(columnPath)
          .flatMap(ParquetColumnChunk::getStatistics)
          .ifPresent(
              stats ->
                  rejectedRangesByColumn
                      .computeIfAbsent(columnPath, unused -> new ArrayList<>())
                      .add(stats));
    }
  }
}
