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
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import java.util.Locale;
import java.util.Map;

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

  static final String PREFETCH_MODE_KEY = "analytics-core.prefetch.mode";
  static final String BUFFER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.prefetch.buffer.cache.max-size-bytes";
  static final String BUFFER_CACHE_TTL_SECONDS_KEY =
      "analytics-core.prefetch.buffer.cache.ttl-seconds";
  static final String BLOCK_SIZE_BYTES_KEY = "analytics-core.prefetch.block.size-bytes";
  static final String HISTORY_MAX_COLUMNS_KEY = "analytics-core.prefetch.history.max-columns";

  private static final PrefetchMode DEFAULT_PREFETCH_MODE = PrefetchMode.DISABLED;
  private static final long DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES = 2L * 1024 * 1024 * 1024; // 2 GB
  private static final long DEFAULT_BUFFER_CACHE_TTL_SECONDS = 60;
  private static final int DEFAULT_BLOCK_SIZE_BYTES = 8 * 1024 * 1024; // 8 MB
  private static final int DEFAULT_HISTORY_MAX_COLUMNS = 256;

  /** Returns the prefetching strategy to apply. Defaults to {@code DISABLED}. */
  public abstract PrefetchMode getPrefetchMode();

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
   * Defaults to {@code 15} columns.
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
        .setBufferCacheMaxSizeBytes(DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES)
        .setBufferCacheTtlSeconds(DEFAULT_BUFFER_CACHE_TTL_SECONDS)
        .setBlockSizeBytes(DEFAULT_BLOCK_SIZE_BYTES)
        .setHistoryMaxColumns(DEFAULT_HISTORY_MAX_COLUMNS);
  }

  /**
   * Creates a {@link GcsPrefetchOptions} instance from a map of configuration options.
   *
   * <p>Keys absent from the map keep their default value. The prefetch mode is matched
   * case-insensitively and hyphens are treated as underscores, so {@code predictive-row-group}
   * resolves to {@code PREDICTIVE_ROW_GROUP}.
   *
   * @param analyticsCoreOptions the configuration options, keyed without the {@code prefix}
   * @param prefix the prefix prepended to every recognised key
   * @throws IllegalArgumentException if the prefetch mode value does not name a {@link
   *     PrefetchMode}
   */
  public static GcsPrefetchOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsPrefetchOptions.Builder optionsBuilder = builder();
    String prefetchModeFullKey = prefix + PREFETCH_MODE_KEY;
    if (analyticsCoreOptions.containsKey(prefetchModeFullKey)) {
      String value = analyticsCoreOptions.get(prefetchModeFullKey);
      try {
        optionsBuilder.setPrefetchMode(
            PrefetchMode.valueOf(value.replace('-', '_').toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid value '%s' for key '%s'", value, prefetchModeFullKey), e);
      }
    }
    String bufferCacheMaxSizeBytesFullKey = prefix + BUFFER_CACHE_MAX_SIZE_BYTES_KEY;
    if (analyticsCoreOptions.containsKey(bufferCacheMaxSizeBytesFullKey)) {
      optionsBuilder.setBufferCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(bufferCacheMaxSizeBytesFullKey)));
    }
    String bufferCacheTtlSecondsFullKey = prefix + BUFFER_CACHE_TTL_SECONDS_KEY;
    if (analyticsCoreOptions.containsKey(bufferCacheTtlSecondsFullKey)) {
      optionsBuilder.setBufferCacheTtlSeconds(
          Long.parseLong(analyticsCoreOptions.get(bufferCacheTtlSecondsFullKey)));
    }
    String blockSizeBytesFullKey = prefix + BLOCK_SIZE_BYTES_KEY;
    if (analyticsCoreOptions.containsKey(blockSizeBytesFullKey)) {
      optionsBuilder.setBlockSizeBytes(
          ConfigurationUtil.safeParseInteger(
              blockSizeBytesFullKey, analyticsCoreOptions.get(blockSizeBytesFullKey)));
    }
    String historyMaxColumnsFullKey = prefix + HISTORY_MAX_COLUMNS_KEY;
    if (analyticsCoreOptions.containsKey(historyMaxColumnsFullKey)) {
      optionsBuilder.setHistoryMaxColumns(
          ConfigurationUtil.safeParseInteger(
              historyMaxColumnsFullKey, analyticsCoreOptions.get(historyMaxColumnsFullKey)));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsPrefetchOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets the prefetching strategy to apply. Defaults to {@code DISABLED}. */
    public abstract Builder setPrefetchMode(PrefetchMode prefetchMode);

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
     * Sets the fixed size (in bytes) of a prefetch block, which is the smallest unit that is
     * fetched from GCS and cached. Defaults to {@code 4194304} (4 MB).
     */
    public abstract Builder setBlockSizeBytes(int blockSizeBytes);

    /**
     * Sets the maximum number of columns tracked per Parquet schema in the access history. Defaults
     * to {@code 15} columns.
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
