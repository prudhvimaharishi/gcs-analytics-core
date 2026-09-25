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
import com.google.common.collect.ImmutableMap;
import java.util.Optional;

/** The byte layout of a single Parquet row group. */
@AutoValue
abstract class ParquetRowGroup {

  /** Returns the zero-based ordinal of this row group within the file. */
  abstract int getOrdinal();

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

  /**
   * Builder for {@link ParquetRowGroup}.
   *
   * <p>The start and end offsets are derived from the added column chunks when {@link #build()} is
   * called, so they are computed once rather than on every access.
   */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setOrdinal(int ordinal);

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
