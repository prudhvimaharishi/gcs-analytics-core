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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** The byte layout of a single Parquet row group. */
@AutoValue
abstract class ParquetRowGroup {

  /** Returns the zero-based index of this row group within the file. */
  abstract int getIndex();

  /** Returns the column chunks of this row group, keyed by dotted column path. */
  abstract ImmutableMap<String, ParquetColumnChunk> getColumnChunks();

  /** Returns the offset of the first byte of this row group, or {@code 0} if it has no chunks. */
  abstract long getStartOffset();

  /**
   * Returns the offset one past the last byte of this row group, or {@code 0} if it has no chunks.
   */
  abstract long getEndOffset();

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
   * Returns the sorted, coalesced byte ranges of this row group: dictionary page ranges for {@code
   * dictionaryColumns} and data ranges for {@code dataColumns} (data-page ranges for columns also
   * in {@code dictionaryColumns}, or whole column chunks otherwise). Dictionary ranges and data
   * ranges are coalesced separately within {@code maxBlockSizeBytes} so a small dictionary page is
   * never merged into an adjacent data download.
   */
  final ImmutableList<Range<Long>> getCoalescedColumnRanges(
      Set<String> dataColumns, Set<String> dictionaryColumns, long maxBlockSizeBytes) {
    List<Range<Long>> dictionaryRanges = collectDictionaryRanges(dictionaryColumns);
    List<Range<Long>> dataRanges = collectDataRanges(dataColumns, dictionaryColumns);
    List<Range<Long>> combinedRanges = new ArrayList<>();
    combinedRanges.addAll(coalesceConnectedRanges(dictionaryRanges, maxBlockSizeBytes));
    combinedRanges.addAll(coalesceConnectedRanges(dataRanges, maxBlockSizeBytes));
    combinedRanges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return ImmutableList.copyOf(combinedRanges);
  }

  private List<Range<Long>> collectDictionaryRanges(Set<String> dictionaryColumns) {
    List<Range<Long>> ranges = new ArrayList<>();
    for (String columnPath : dictionaryColumns) {
      getColumnChunk(columnPath)
          .flatMap(ParquetColumnChunk::getDictionaryPageRange)
          .ifPresent(ranges::add);
    }
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return ranges;
  }

  private List<Range<Long>> collectDataRanges(
      Set<String> dataColumns, Set<String> dictionaryColumns) {
    List<Range<Long>> ranges = new ArrayList<>();
    for (String columnPath : dataColumns) {
      Optional<ParquetColumnChunk> maybeChunk = getColumnChunk(columnPath);
      if (!maybeChunk.isPresent()) {
        continue;
      }
      ParquetColumnChunk chunk = maybeChunk.get();
      if (dictionaryColumns.contains(columnPath) && chunk.getDictionaryPageRange().isPresent()) {
        if (chunk.getDataPageOffset() < chunk.getEndOffset()) {
          ranges.add(chunk.getDataPageRange());
        }
      } else {
        ranges.add(chunk.getByteRange());
      }
    }
    ranges.sort(Comparator.comparingLong(Range::lowerEndpoint));
    return ranges;
  }

  private static ImmutableList<Range<Long>> coalesceConnectedRanges(
      List<Range<Long>> sortedRanges, long maxBlockSizeBytes) {
    if (sortedRanges.isEmpty()) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<Range<Long>> coalesced = ImmutableList.builder();
    Range<Long> current = sortedRanges.get(0);
    for (int i = 1; i < sortedRanges.size(); i++) {
      Range<Long> next = sortedRanges.get(i);
      if (current.isConnected(next)
          && next.upperEndpoint() - current.lowerEndpoint() <= maxBlockSizeBytes) {
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
   * Builder for {@link ParquetRowGroup}.
   *
   * <p>The start and end offsets are derived from the added column chunks when {@link #build()} is
   * called, so they are computed once rather than on every access.
   */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setIndex(int index);

    abstract ImmutableMap.Builder<String, ParquetColumnChunk> columnChunksBuilder();

    abstract Builder setStartOffset(long startOffset);

    abstract Builder setEndOffset(long endOffset);

    abstract ParquetRowGroup autoBuild();

    private long minStartOffset = Long.MAX_VALUE;
    private long maxEndOffset = Long.MIN_VALUE;

    final Builder addColumnChunk(ParquetColumnChunk columnChunk) {
      columnChunksBuilder().put(columnChunk.getColumnPath(), columnChunk);
      minStartOffset = Math.min(minStartOffset, columnChunk.getStartOffset());
      maxEndOffset = Math.max(maxEndOffset, columnChunk.getEndOffset());
      return this;
    }

    final ParquetRowGroup build() {
      boolean hasChunks = minStartOffset != Long.MAX_VALUE;
      setStartOffset(hasChunks ? minStartOffset : 0);
      setEndOffset(hasChunks ? maxEndOffset : 0);
      return autoBuild();
    }
  }
}
