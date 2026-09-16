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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * <p>This class is thread-safe.
 */
public final class SchemaAccessHistory {

  private static final int MAX_TRACKED_SCHEMAS = 1024;

  private static final ConcurrentMap<Integer, SchemaAccessHistory> SHARED_INSTANCES =
      new ConcurrentHashMap<>();

  private final int maxColumnsPerSchema;
  private final Cache<Integer, TrackedColumns> historyBySchema;

  /**
   * Creates a history.
   *
   * @param maxColumnsPerSchema the maximum number of columns tracked for a single schema
   */
  public SchemaAccessHistory(int maxColumnsPerSchema) {
    checkArgument(maxColumnsPerSchema > 0, "maxColumnsPerSchema must be positive");
    this.maxColumnsPerSchema = maxColumnsPerSchema;
    this.historyBySchema = Caffeine.newBuilder().maximumSize(MAX_TRACKED_SCHEMAS).build();
  }

  /**
   * Returns the history shared by the whole process for the given capacity.
   *
   * <p>Query engines hand every task its own file system instance, so a history owned by that
   * instance is discarded as soon as the task ends and each task relearns the same columns from
   * scratch. Sharing one history per process instead lets the first task of a scan teach every task
   * that follows, which is what makes files after the first one prefetchable.
   *
   * @param maxColumnsPerSchema the maximum number of columns tracked for a single schema
   */
  public static SchemaAccessHistory getSharedInstance(int maxColumnsPerSchema) {
    checkArgument(maxColumnsPerSchema > 0, "maxColumnsPerSchema must be positive");
    return SHARED_INSTANCES.computeIfAbsent(maxColumnsPerSchema, SchemaAccessHistory::new);
  }

  /** Returns the columns previously read into their data pages for the given schema. */
  public ImmutableSet<String> getDataColumns(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null ? ImmutableSet.of() : ImmutableSet.copyOf(tracked.dataColumns);
  }

  /** Returns the columns previously read only as far as their dictionary page. */
  public ImmutableSet<String> getDictionaryColumns(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null ? ImmutableSet.of() : ImmutableSet.copyOf(tracked.dictionaryColumns);
  }

  /** Records that the data pages of {@code columnPath} were read. */
  public void recordDataAccess(int schemaFingerprint, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    TrackedColumns tracked = trackedColumnsFor(schemaFingerprint);
    addBounded(tracked.dataColumns, columnPath);
    tracked.dictionaryColumns.remove(columnPath);
  }

  /**
   * Records that only the dictionary page of {@code columnPath} was read.
   *
   * <p>Ignored once the column is known to be read in full, since escalation to the data pages is
   * one-way for a given schema.
   */
  public void recordDictionaryAccess(int schemaFingerprint, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    TrackedColumns tracked = trackedColumnsFor(schemaFingerprint);
    if (tracked.dataColumns.contains(columnPath)) {
      return;
    }
    addBounded(tracked.dictionaryColumns, columnPath);
  }

  /** Discards all recorded history. */
  public void invalidateAll() {
    historyBySchema.invalidateAll();
  }

  private TrackedColumns trackedColumnsFor(int schemaFingerprint) {
    return historyBySchema.get(schemaFingerprint, fingerprint -> new TrackedColumns());
  }

  private void addBounded(Set<String> columns, String columnPath) {
    if (columns.size() >= maxColumnsPerSchema && !columns.contains(columnPath)) {
      return;
    }
    columns.add(columnPath);
  }

  private static final class TrackedColumns {
    private final Set<String> dataColumns = ConcurrentHashMap.newKeySet();
    private final Set<String> dictionaryColumns = ConcurrentHashMap.newKeySet();
  }
}
