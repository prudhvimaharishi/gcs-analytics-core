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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** The byte layout of a single Parquet row group, whose column chunks never share a byte. */
@AutoValue
abstract class ParquetRowGroup {

  /**
   * The longest range that this class returns. Some JVMs reserve header words in every array, so
   * this is the largest array length that all of them can allocate.
   */
  private static final long MAX_RANGE_BYTES = Integer.MAX_VALUE - 8;

  /** Returns the zero-based index of this row group within the file. */
  abstract int getIndex();

  /** Returns the column chunks of this row group, keyed by dotted column path. */
  abstract ImmutableMap<String, ParquetColumnChunk> getColumnChunks();

  /** Returns the offset of the first byte of this row group, or {@code 0} if it has no chunks. */
  @Memoized
  long getStartOffset() {
    return getColumnChunks().values().stream()
        .mapToLong(ParquetColumnChunk::getStartOffset)
        .min()
        .orElse(0);
  }

  /**
   * Returns the offset one past the last byte of this row group, or {@code 0} if it has no chunks.
   */
  @Memoized
  long getEndOffset() {
    return getColumnChunks().values().stream()
        .mapToLong(ParquetColumnChunk::getEndOffset)
        .max()
        .orElse(0);
  }

  static Builder builder() {
    return new AutoValue_ParquetRowGroup.Builder();
  }

  /** Returns the chunk for the given dotted column path, if this row group contains it. */
  final Optional<ParquetColumnChunk> getColumnChunk(String columnPath) {
    return Optional.ofNullable(getColumnChunks().get(columnPath));
  }

  /** Returns whether {@code position} falls inside this row group. */
  final boolean containsPosition(long position) {
    return position >= getStartOffset() && position < getEndOffset();
  }

  /**
   * Returns the paths of every column chunk in this row group whose data pages overlap {@code
   * [startOffset, endOffset)}.
   */
  final ImmutableSet<String> findDataColumnsInRange(long startOffset, long endOffset) {
    ImmutableSet.Builder<String> touchedColumns = ImmutableSet.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.touchesDataPages(startOffset, endOffset)) {
        touchedColumns.add(chunk.getColumnPath());
      }
    }
    return touchedColumns.build();
  }

  /**
   * Returns the paths of every column chunk in this row group whose dictionary page overlaps {@code
   * [startOffset, endOffset)}.
   */
  final ImmutableSet<String> findDictionaryColumnsInRange(long startOffset, long endOffset) {
    ImmutableSet.Builder<String> touchedColumns = ImmutableSet.builder();
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.touchesDictionaryPage(startOffset, endOffset)) {
        touchedColumns.add(chunk.getColumnPath());
      }
    }
    return touchedColumns.build();
  }

  /** Returns the data-page byte range of the column chunk containing {@code position}, if any. */
  final Optional<Range<Long>> findDataPageRangeAt(long position) {
    for (ParquetColumnChunk chunk : getColumnChunks().values()) {
      if (chunk.containsDataPagePosition(position)) {
        return Optional.of(chunk.getDataPageRange());
      }
    }
    return Optional.empty();
  }

  /**
   * Returns the sorted byte ranges of the whole column chunks of {@code columns}, dictionary pages
   * included.
   *
   * <p>Touching chunks merge into one range. Each range fits in a single array: a chunk longer than
   * {@code MAX_RANGE_BYTES} comes back as several consecutive ranges.
   */
  final ImmutableList<Range<Long>> getCoalescedColumnRanges(Set<String> columns) {
    List<Range<Long>> ranges = new ArrayList<>();
    for (String columnPath : columns) {
      getColumnChunk(columnPath).map(ParquetColumnChunk::getByteRange).ifPresent(ranges::add);
    }
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return splitOversizedRanges(coalesceConnectedRanges(ranges));
  }

  /**
   * Returns the sorted dictionary page byte ranges of the dictionary-encoded {@code columns}.
   *
   * <p>Each range fits in a single array: a dictionary page longer than {@code MAX_RANGE_BYTES}
   * comes back as several consecutive ranges.
   */
  final ImmutableList<Range<Long>> getDictionaryPageRanges(Set<String> columns) {
    List<Range<Long>> ranges = new ArrayList<>();
    for (String columnPath : columns) {
      getColumnChunk(columnPath)
          .flatMap(ParquetColumnChunk::getDictionaryPageRange)
          .ifPresent(ranges::add);
    }
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return splitOversizedRanges(ranges);
  }

  private static ImmutableList<Range<Long>> coalesceConnectedRanges(
      List<Range<Long>> sortedRanges) {
    if (sortedRanges.isEmpty()) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<Range<Long>> coalesced = ImmutableList.builder();
    Range<Long> current = sortedRanges.get(0);
    for (int i = 1; i < sortedRanges.size(); i++) {
      Range<Long> next = sortedRanges.get(i);
      if (current.isConnected(next)
          && next.upperEndpoint() - current.lowerEndpoint() <= MAX_RANGE_BYTES) {
        current = current.span(next);
      } else {
        coalesced.add(current);
        current = next;
      }
    }
    coalesced.add(current);
    return coalesced.build();
  }

  /**
   * Splits every range longer than {@code MAX_RANGE_BYTES} into consecutive ranges of at most that
   * length, keeping the order of {@code ranges}.
   */
  private static ImmutableList<Range<Long>> splitOversizedRanges(List<Range<Long>> ranges) {
    ImmutableList.Builder<Range<Long>> splitRanges = ImmutableList.builder();
    for (Range<Long> range : ranges) {
      long start = range.lowerEndpoint();
      long end = range.upperEndpoint();
      while (end - start > MAX_RANGE_BYTES) {
        splitRanges.add(Range.closedOpen(start, start + MAX_RANGE_BYTES));
        start += MAX_RANGE_BYTES;
      }
      splitRanges.add(Range.closedOpen(start, end));
    }
    return splitRanges.build();
  }

  /** Builder for {@link ParquetRowGroup}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setIndex(int index);

    abstract ImmutableMap.Builder<String, ParquetColumnChunk> columnChunksBuilder();

    abstract ParquetRowGroup autoBuild();

    final Builder addColumnChunk(ParquetColumnChunk columnChunk) {
      columnChunksBuilder().put(columnChunk.getColumnPath(), columnChunk);
      return this;
    }

    /**
     * Builds the row group.
     *
     * @throws IllegalArgumentException if two column chunks share a column path
     * @throws IllegalStateException if the index is negative or two column chunks share a byte
     */
    final ParquetRowGroup build() {
      ParquetRowGroup rowGroup = autoBuild();
      checkState(
          rowGroup.getIndex() >= 0,
          "Row group index must not be negative: %s",
          rowGroup.getIndex());
      checkColumnChunksDoNotOverlap(rowGroup.getColumnChunks().values());
      return rowGroup;
    }

    private static void checkColumnChunksDoNotOverlap(Collection<ParquetColumnChunk> columnChunks) {
      List<ParquetColumnChunk> sortedChunks = new ArrayList<>(columnChunks);
      sortedChunks.sort(Comparator.comparingLong(ParquetColumnChunk::getStartOffset));
      for (int i = 1; i < sortedChunks.size(); i++) {
        ParquetColumnChunk previous = sortedChunks.get(i - 1);
        ParquetColumnChunk current = sortedChunks.get(i);
        checkState(
            previous.getEndOffset() <= current.getStartOffset(),
            "Column chunks must not overlap: %s and %s",
            previous,
            current);
      }
    }
  }
}
