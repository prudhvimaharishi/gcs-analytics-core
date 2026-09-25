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

  /** Outcome of evaluating row-group filters on an opened Parquet stream. */
  public enum FileFilterOutcome {
    /** Stream closed after footer read without reading any dictionary or data page. */
    FOOTER_REJECTED,
    /** Stream read dictionary page(s) but closed without reading any data page. */
    DICT_REJECTED,
    /** Stream read at least one data page. */
    SURVIVED
  }

  private static final int MAX_TRACKED_SCHEMAS = 1024;
  private static final int OUTCOME_WINDOW_SIZE = 8;

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
  }

  /**
   * Records that the dictionary page of {@code columnPath} was read (for example, during row-group
   * dictionary filter evaluation).
   */
  public void recordDictionaryAccess(int schemaFingerprint, String columnPath) {
    checkNotNull(columnPath, "columnPath cannot be null");
    TrackedColumns tracked = trackedColumnsFor(schemaFingerprint);
    addBounded(tracked.dictionaryColumns, columnPath);
  }

  /** Records the filter survival outcome of a stream for the given schema. */
  public void recordFileOutcome(int schemaFingerprint, FileFilterOutcome outcome) {
    checkNotNull(outcome, "outcome cannot be null");
    TrackedColumns tracked = trackedColumnsFor(schemaFingerprint);
    tracked.recordOutcome(outcome);
  }

  /**
   * Returns whether at least half of recent files for {@code schemaFingerprint} survived the
   * footer-level min/max filter (`footerPassRate >= 0.50`).
   */
  public boolean shouldSpeculateAtFooter(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null || tracked.shouldSpeculateAtFooter();
  }

  /**
   * Returns whether at least half of recent files that passed the footer filter also survived the
   * dictionary filter (`dictPassRate >= 0.50`).
   */
  public boolean shouldSpeculateOnDictionary(int schemaFingerprint) {
    TrackedColumns tracked = historyBySchema.getIfPresent(schemaFingerprint);
    return tracked == null || tracked.shouldSpeculateOnDictionary();
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
    private final java.util.ArrayDeque<FileFilterOutcome> recentFooterOutcomes =
        new java.util.ArrayDeque<>(OUTCOME_WINDOW_SIZE);
    private final java.util.ArrayDeque<FileFilterOutcome> recentDictOutcomes =
        new java.util.ArrayDeque<>(OUTCOME_WINDOW_SIZE);

    synchronized void recordOutcome(FileFilterOutcome outcome) {
      if (recentFooterOutcomes.size() >= OUTCOME_WINDOW_SIZE) {
        recentFooterOutcomes.pollFirst();
      }
      recentFooterOutcomes.addLast(outcome);
      if (outcome != FileFilterOutcome.FOOTER_REJECTED) {
        if (recentDictOutcomes.size() >= OUTCOME_WINDOW_SIZE) {
          recentDictOutcomes.pollFirst();
        }
        recentDictOutcomes.addLast(outcome);
      }
    }

    synchronized boolean shouldSpeculateAtFooter() {
      if (recentFooterOutcomes.size() < 2) {
        return true;
      }
      int passedFooter = 0;
      for (FileFilterOutcome outcome : recentFooterOutcomes) {
        if (outcome != FileFilterOutcome.FOOTER_REJECTED) {
          passedFooter++;
        }
      }
      return passedFooter * 2 >= recentFooterOutcomes.size();
    }

    synchronized boolean shouldSpeculateOnDictionary() {
      if (recentDictOutcomes.size() < 2) {
        return true;
      }
      int survived = 0;
      for (FileFilterOutcome outcome : recentDictOutcomes) {
        if (outcome == FileFilterOutcome.SURVIVED) {
          survived++;
        }
      }
      return survived * 2 >= recentDictOutcomes.size();
    }
  }
}
