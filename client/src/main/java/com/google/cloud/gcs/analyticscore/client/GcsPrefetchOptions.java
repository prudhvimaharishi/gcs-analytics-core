/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

/** Configuration options for predictive prefetching. */
@AutoValue
public abstract class GcsPrefetchOptions {

  /** Strategy used to decide which bytes are speculatively fetched ahead of the engine. */
  public enum PrefetchMode {
    /**
     * Learns which columns a query touches for a given Parquet schema and prefetches those columns
     * for the current and the next row group.
     */
    PREDICTIVE_ROW_GROUP,

    /** Turns predictive prefetching off. */
    DISABLED
  }

  /** Dictionary page read that triggers prefetching a row group's data pages. */
  public enum DictionaryTrigger {
    /** Prefetches once any known dictionary column of the row group is read. */
    FIRST_DICT_READ,

    /** Prefetches once every known dictionary column of the row group is read. */
    LAST_DICT_READ
  }

  private static final PrefetchMode DEFAULT_PREFETCH_MODE = PrefetchMode.DISABLED;
  private static final DictionaryTrigger DEFAULT_DICTIONARY_TRIGGER =
      DictionaryTrigger.LAST_DICT_READ;
  private static final long DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES = 2L * 1024 * 1024 * 1024; // 2 GB
  private static final long DEFAULT_BUFFER_CACHE_TTL_SECONDS = 60;
  private static final int DEFAULT_BLOCK_SIZE_BYTES = 8 * 1024 * 1024; // 8 MB
  private static final int DEFAULT_HISTORY_MAX_COLUMNS = 256;

  /** Returns the prefetching strategy to apply. Defaults to {@code DISABLED}. */
  public abstract PrefetchMode getPrefetchMode();

  /**
   * Returns the dictionary page read that triggers prefetching a row group's data pages. Defaults
   * to {@code LAST_DICT_READ}.
   */
  public abstract DictionaryTrigger getDictionaryTrigger();

  /**
   * Returns the maximum total capacity (in bytes) of the prefetch buffer cache. Defaults to {@code
   * 2147483648} (2 GB).
   */
  public abstract long getBufferCacheMaxSizeBytes();

  /**
   * Returns how long (in seconds) a prefetched block is retained in the buffer cache after it was
   * last read or written. Defaults to {@code 60} seconds.
   */
  public abstract long getBufferCacheTtlSeconds();

  /**
   * Returns the maximum size (in bytes) of a single prefetched byte range slice. Defaults to {@code
   * 8388608} (8 MB).
   */
  public abstract int getBlockSizeBytes();

  /**
   * Returns the maximum number of columns tracked per Parquet schema in the access history.
   * Defaults to {@code 256} columns.
   */
  public abstract int getHistoryMaxColumns();

  /**
   * Returns whether predictive prefetching is enabled, that is whether the prefetch mode is
   * anything other than {@code DISABLED}.
   */
  public final boolean isEnabled() {
    return getPrefetchMode() != PrefetchMode.DISABLED;
  }

  /**
   * Returns a builder for {@link GcsPrefetchOptions} with the same property values as this
   * instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsPrefetchOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsPrefetchOptions.Builder()
        .setPrefetchMode(DEFAULT_PREFETCH_MODE)
        .setDictionaryTrigger(DEFAULT_DICTIONARY_TRIGGER)
        .setBufferCacheMaxSizeBytes(DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES)
        .setBufferCacheTtlSeconds(DEFAULT_BUFFER_CACHE_TTL_SECONDS)
        .setBlockSizeBytes(DEFAULT_BLOCK_SIZE_BYTES)
        .setHistoryMaxColumns(DEFAULT_HISTORY_MAX_COLUMNS);
  }

  /** Builder for {@link GcsPrefetchOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets the prefetching strategy to apply. Defaults to {@code DISABLED}. */
    public abstract Builder setPrefetchMode(PrefetchMode prefetchMode);

    /**
     * Sets the dictionary page read that triggers prefetching a row group's data pages. Defaults to
     * {@code LAST_DICT_READ}.
     */
    public abstract Builder setDictionaryTrigger(DictionaryTrigger dictionaryTrigger);

    /**
     * Sets the maximum total capacity (in bytes) of the prefetch buffer cache. Defaults to {@code
     * 2147483648} (2 GB).
     */
    public abstract Builder setBufferCacheMaxSizeBytes(long bufferCacheMaxSizeBytes);

    /**
     * Sets how long (in seconds) a prefetched block is retained in the buffer cache after it was
     * last read or written. Defaults to {@code 60} seconds.
     */
    public abstract Builder setBufferCacheTtlSeconds(long bufferCacheTtlSeconds);

    /**
     * Sets the maximum size (in bytes) of a single prefetched byte range slice. Defaults to {@code
     * 8388608} (8 MB).
     */
    public abstract Builder setBlockSizeBytes(int blockSizeBytes);

    /**
     * Sets the maximum number of columns tracked per Parquet schema in the access history. Defaults
     * to {@code 256} columns.
     */
    public abstract Builder setHistoryMaxColumns(int historyMaxColumns);

    abstract GcsPrefetchOptions autoBuild();

    /**
     * Builds the {@link GcsPrefetchOptions} instance.
     *
     * @throws IllegalArgumentException if {@code bufferCacheMaxSizeBytes}, {@code
     *     bufferCacheTtlSeconds}, {@code blockSizeBytes} or {@code historyMaxColumns} is
     *     non-positive while the prefetch mode is not {@code DISABLED}
     */
    public GcsPrefetchOptions build() {
      GcsPrefetchOptions options = autoBuild();
      if (options.isEnabled()) {
        checkArgument(
            options.getBufferCacheMaxSizeBytes() > 0,
            "bufferCacheMaxSizeBytes must be positive when prefetch is enabled");
        checkArgument(
            options.getBufferCacheTtlSeconds() > 0,
            "bufferCacheTtlSeconds must be positive when prefetch is enabled");
        checkArgument(
            options.getBlockSizeBytes() > 0,
            "blockSizeBytes must be positive when prefetch is enabled");
        checkArgument(
            options.getHistoryMaxColumns() > 0,
            "historyMaxColumns must be positive when prefetch is enabled");
      }
      return options;
    }
  }
}
