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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Tracks which dictionary pages and data pages of each row group a single Parquet stream has read.
 */
final class RowGroupFilterTracker {

  private final SortedSet<Integer> dataTouchedOrdinals = new TreeSet<>();
  private final Map<Integer, Set<String>> dictionaryColumnsByRowGroup = new HashMap<>();

  /** Records that a dictionary page in {@code rowGroupOrdinal} was read for {@code columnPath}. */
  void recordDictionaryRead(int rowGroupOrdinal, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    dictionaryColumnsByRowGroup
        .computeIfAbsent(rowGroupOrdinal, unused -> new HashSet<>())
        .add(columnPath);
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

  /** Records that data pages in {@code rowGroupOrdinal} were read. */
  void recordDataRead(int rowGroupOrdinal) {
    dataTouchedOrdinals.add(rowGroupOrdinal);
  }
}
