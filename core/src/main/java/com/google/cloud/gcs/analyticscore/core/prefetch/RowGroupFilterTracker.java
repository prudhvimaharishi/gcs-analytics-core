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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
 * <p>Engines such as Apache Iceberg evaluate footer min/max statistics across all row groups at
 * file open and immediately read the dictionary pages of surviving candidate row groups. A row
 * group skipped during this upfront dictionary sweep, whose filter columns all have dictionary
 * pages, is predicted to have failed the min/max check.
 *
 * <p>The prediction can be wrong, for example when an earlier row group was rejected by a filter
 * column this tracker does not know about. A wrong prediction only costs a missed prefetch; it
 * never changes what the engine reads.
 */
final class RowGroupFilterTracker {

  private final SortedSet<Integer> dictionaryTouchedIndices = new TreeSet<>();
  private final SortedSet<Integer> dataTouchedIndices = new TreeSet<>();
  private final Map<Integer, Set<String>> dictionaryColumnsByRowGroup = new HashMap<>();
  private final Set<String> filterColumnPaths = new HashSet<>();

  /** Records that a dictionary page in {@code rowGroupIndex} was read for {@code columnPath}. */
  void recordDictionaryRead(int rowGroupIndex, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    if (dataTouchedIndices.contains(rowGroupIndex)) {
      return;
    }
    dictionaryTouchedIndices.add(rowGroupIndex);
    dictionaryColumnsByRowGroup
        .computeIfAbsent(rowGroupIndex, unused -> new HashSet<>())
        .add(columnPath);
    filterColumnPaths.add(columnPath);
  }

  /**
   * Returns whether all columns in {@code knownDictionaryColumns} that actually have a dictionary
   * page in {@code rowGroupIndex} have been read.
   */
  boolean hasReadAllDictionaries(
      ParquetFileLayout layout, int rowGroupIndex, Set<String> knownDictionaryColumns) {
    checkNotNull(layout, "layout cannot be null");
    checkNotNull(knownDictionaryColumns, "knownDictionaryColumns cannot be null");
    if (knownDictionaryColumns.isEmpty()) {
      return false;
    }
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupIndex);
    if (!rowGroup.isPresent()) {
      return false;
    }
    Set<String> readColumns = dictionaryColumnsByRowGroup.get(rowGroupIndex);
    if (readColumns == null) {
      return false;
    }
    boolean anyDictionaryPresent = false;
    for (String columnPath : knownDictionaryColumns) {
      boolean hasDictionary =
          rowGroup
              .get()
              .getColumnChunk(columnPath)
              .map(ParquetColumnChunk::hasDictionaryPage)
              .orElse(false);
      if (hasDictionary) {
        anyDictionaryPresent = true;
        if (!readColumns.contains(columnPath)) {
          return false;
        }
      }
    }
    return anyDictionaryPresent;
  }

  /**
   * Returns whether any of {@code knownDictionaryColumns} has been read in {@code rowGroupIndex}.
   */
  boolean hasReadAnyDictionary(int rowGroupIndex, Set<String> knownDictionaryColumns) {
    Set<String> readColumns = dictionaryColumnsByRowGroup.get(rowGroupIndex);
    return readColumns != null && !Collections.disjoint(readColumns, knownDictionaryColumns);
  }

  /** Returns the columns whose dictionary page has been read in {@code rowGroupIndex}. */
  Set<String> getDictionaryColumnsRead(int rowGroupIndex) {
    Set<String> readColumns = dictionaryColumnsByRowGroup.get(rowGroupIndex);
    return readColumns == null ? Collections.emptySet() : Collections.unmodifiableSet(readColumns);
  }

  /** Returns the number of distinct row groups whose dictionary pages have been read. */
  int getDictionaryTouchedCount() {
    return dictionaryTouchedIndices.size();
  }

  /** Returns the number of distinct row groups whose data pages have been read. */
  int getDataTouchedCount() {
    return dataTouchedIndices.size();
  }

  /** Returns the highest row group index whose data pages have been read, or {@code -1}. */
  int getLastDataReadIndex() {
    return dataTouchedIndices.isEmpty() ? -1 : dataTouchedIndices.last();
  }

  /** Records that data pages in {@code rowGroupIndex} were read. */
  void recordDataRead(int rowGroupIndex) {
    dataTouchedIndices.add(rowGroupIndex);
  }

  /**
   * Returns the next row group index strictly after {@code afterIndex} that has not been eliminated
   * by upfront footer pruning, or empty if no remaining row group survives.
   */
  OptionalInt findNextSurvivingRowGroup(
      ParquetFileLayout layout, int afterIndex, Set<String> knownDictionaryColumns) {
    checkNotNull(layout, "layout cannot be null");
    checkNotNull(knownDictionaryColumns, "knownDictionaryColumns cannot be null");
    if (dictionaryTouchedIndices.isEmpty()) {
      return OptionalInt.empty();
    }

    Set<String> activeFilterColumns = getActiveFilterColumns(knownDictionaryColumns);
    int rowGroupCount = layout.getRowGroups().size();
    int maxDictIndex = dictionaryTouchedIndices.last();

    for (int candidate = afterIndex + 1; candidate < rowGroupCount; candidate++) {
      if (isSkippedByUpfrontDictionarySweep(layout, candidate, afterIndex, activeFilterColumns)) {
        if (candidate > maxDictIndex) {
          return OptionalInt.empty();
        }
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
      ParquetFileLayout layout, int candidate, int afterIndex, Set<String> activeFilterColumns) {
    Set<String> expectedColumns =
        dictionaryColumnsByRowGroup.getOrDefault(afterIndex, Collections.emptySet());
    if (expectedColumns.isEmpty()) {
      expectedColumns = activeFilterColumns;
    }
    return !hasReadAllDictionaries(layout, candidate, expectedColumns)
        && allFilterColumnsHaveDictionaryPage(layout, candidate, activeFilterColumns);
  }

  private boolean allFilterColumnsHaveDictionaryPage(
      ParquetFileLayout layout, int index, Set<String> activeFilterColumns) {
    if (activeFilterColumns.isEmpty()) {
      return false;
    }
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(index);
    if (!rowGroup.isPresent()) {
      return false;
    }
    for (String columnPath : activeFilterColumns) {
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

  private Set<String> getActiveFilterColumns(Set<String> knownDictionaryColumns) {
    Set<String> activeFilterColumns = new HashSet<>(filterColumnPaths);
    activeFilterColumns.addAll(knownDictionaryColumns);
    return activeFilterColumns;
  }
}
