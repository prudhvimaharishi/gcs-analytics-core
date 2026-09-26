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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;

/**
 * Records which columns an engine actually reads, keyed by a schema fingerprint.
 *
 * <p>Because the fingerprint is derived from the schema rather than the object name, history
 * learned while reading one file is reused when a later file with the same schema is opened. This
 * is what allows the second and subsequent files of a multi-file scan to be prefetched.
 *
 * <p>Columns are tracked separately depending on whether the engine read into the data pages or
 * only touched the dictionary page, so that a column used purely for filter evaluation is not
 * speculatively fetched in full.
 *
 * <p>The columns of each schema are bounded by a frequency-based (W-TinyLFU) policy: once a schema
 * tracks {@link #MAX_COLUMNS_PER_SCHEMA} columns, rarely read columns are evicted in favour of
 * frequently read ones, so the history follows the columns queries currently read.
 *
 * <p>This class is thread-safe.
 */
public final class SchemaAccessHistory {

  private static final int MAX_TRACKED_SCHEMAS = 1024;

  /** The maximum number of columns tracked for a single schema. */
  static final int MAX_COLUMNS_PER_SCHEMA = 256;

  private final Cache<Integer, TrackedColumns> historyBySchema;

  /** Creates an empty history. */
  public SchemaAccessHistory() {
    this.historyBySchema = Caffeine.newBuilder().maximumSize(MAX_TRACKED_SCHEMAS).build();
  }

  /** Returns the columns previously read into their data pages for the given schema. */
  public ImmutableSet<String> getDataColumns(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null
        ? ImmutableSet.of()
        : ImmutableSet.copyOf(tracked.dataColumns.asMap().keySet());
  }

  /** Returns the columns previously read only as far as their dictionary page. */
  public ImmutableSet<String> getDictionaryColumns(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null
        ? ImmutableSet.of()
        : ImmutableSet.copyOf(tracked.dictionaryColumns.asMap().keySet());
  }

  /**
   * Records that the data pages of {@code columnPath} were read. Each call counts as one use of the
   * column when deciding which columns to keep.
   */
  public void recordDataAccess(int schemaFingerprint, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    Boolean unused =
        trackedColumnsFor(schemaFingerprint).dataColumns.get(columnPath, column -> Boolean.TRUE);
  }

  /**
   * Records that the dictionary page of {@code columnPath} was read (for example, during row-group
   * dictionary filter evaluation).
   */
  public void recordDictionaryAccess(int schemaFingerprint, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    Boolean unused =
        trackedColumnsFor(schemaFingerprint)
            .dictionaryColumns
            .get(columnPath, column -> Boolean.TRUE);
  }

  /** Discards all recorded history. */
  public void invalidateAll() {
    historyBySchema.invalidateAll();
  }

  private TrackedColumns trackedColumnsFor(int schemaFingerprint) {
    return historyBySchema.get(schemaFingerprint, fingerprint -> new TrackedColumns());
  }

  /**
   * Creates the column cache of one schema. Maintenance runs on the caller thread so the size bound
   * holds immediately after every write; the cache is small, so this work is cheap.
   */
  private static Cache<String, Boolean> newColumnCache() {
    return Caffeine.newBuilder()
        .maximumSize(MAX_COLUMNS_PER_SCHEMA)
        .executor(Runnable::run)
        .build();
  }

  private static final class TrackedColumns {
    private final Cache<String, Boolean> dataColumns = newColumnCache();
    private final Cache<String, Boolean> dictionaryColumns = newColumnCache();
  }
}
