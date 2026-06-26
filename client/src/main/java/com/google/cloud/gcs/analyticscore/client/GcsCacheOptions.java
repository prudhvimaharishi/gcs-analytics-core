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
import java.util.Map;

/** Configuration options for the GCS caching layer. */
@AutoValue
public abstract class GcsCacheOptions {

  private static final String FOOTER_CACHE_ENABLED_KEY = "analytics-core.footer.cache.enabled";
  private static final String FOOTER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.footer.cache.max-size-bytes";
  private static final String SMALL_FILE_CACHE_ENABLED_KEY =
      "analytics-core.small-file.cache.enabled";
  private static final String SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.small-file.cache.max-size-bytes";
  private static final String OBJECT_CHUNK_CACHE_ENABLED_KEY =
      "analytics-core.object.cache.enabled";
  private static final String OBJECT_CHUNK_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.object.cache.max-size-bytes";
  private static final String OBJECT_CHUNK_SIZE_BYTES_KEY =
      "analytics-core.object.cache.chunk-size-bytes";
  private static final String OBJECT_CHUNK_MAX_FETCH_SPLITS_KEY =
      "analytics-core.object.cache.max-fetch-splits";

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;
  private static final long GB = 1024L * MB;

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = false;
  private static final long DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES = 100 * MB;
  private static final boolean DEFAULT_SMALL_OBJECT_CACHE_ENABLED = false;
  private static final long DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES = 200 * MB;
  private static final boolean DEFAULT_OBJECT_CHUNK_CACHE_ENABLED = true;
  private static final long DEFAULT_OBJECT_CHUNK_CACHE_MAX_SIZE_BYTES = GB;
  private static final int DEFAULT_OBJECT_CHUNK_SIZE_BYTES = 128 * (int) KB;
  private static final int DEFAULT_OBJECT_CHUNK_MAX_FETCH_SPLITS = 3;

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the Parquet footer cache. */
  public abstract long getFooterCacheMaxSizeBytes();

  /** Returns the maximum capacity (in bytes) to hold in the small object cache. */
  /** Returns whether the small object cache is enabled. */
  public abstract boolean isSmallObjectCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the small object cache. */
  public abstract long getSmallObjectCacheMaxSizeBytes();

  /**
   * Returns whether the object-chunk cache is enabled. When enabled, objects are cached in
   * fixed-size chunks so that repeated or nearby reads are served from memory.
   */
  public abstract boolean isObjectChunkCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the object-chunk cache. */
  public abstract long getObjectChunkCacheMaxSizeBytes();

  /** Returns the chunk size (in bytes) the object is partitioned into for caching. */
  public abstract int getObjectChunkSizeBytes();

  /**
   * Returns the maximum number of separate source requests a single read may issue to fetch its
   * missing chunks. When a read's missing chunks are fragmented across more than this many
   * non-contiguous runs, the optimizer fetches the whole covering range with one request
   * (re-reading any already-cached gaps) instead, trading some redundant bytes for fewer round
   * trips.
   */
  public abstract int getObjectChunkMaxFetchSplits();

  /**
   * Returns a builder for {@link GcsCacheOptions} with the same property values as this instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsCacheOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsCacheOptions.Builder()
        .setFooterCacheEnabled(DEFAULT_FOOTER_CACHE_ENABLED)
        .setFooterCacheMaxSizeBytes(DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES)
        .setSmallObjectCacheEnabled(DEFAULT_SMALL_OBJECT_CACHE_ENABLED)
        .setSmallObjectCacheMaxSizeBytes(DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES)
        .setObjectChunkCacheEnabled(DEFAULT_OBJECT_CHUNK_CACHE_ENABLED)
        .setObjectChunkCacheMaxSizeBytes(DEFAULT_OBJECT_CHUNK_CACHE_MAX_SIZE_BYTES)
        .setObjectChunkSizeBytes(DEFAULT_OBJECT_CHUNK_SIZE_BYTES)
        .setObjectChunkMaxFetchSplits(DEFAULT_OBJECT_CHUNK_MAX_FETCH_SPLITS);
  }

  /** Creates a {@link GcsCacheOptions} instance from a map of configuration options. */
  public static GcsCacheOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsCacheOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_ENABLED_KEY)) {
      optionsBuilder.setFooterCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setFooterCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_FILE_CACHE_ENABLED_KEY)) {
      optionsBuilder.setSmallObjectCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + SMALL_FILE_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setSmallObjectCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + OBJECT_CHUNK_CACHE_ENABLED_KEY)) {
      optionsBuilder.setObjectChunkCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + OBJECT_CHUNK_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + OBJECT_CHUNK_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setObjectChunkCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + OBJECT_CHUNK_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + OBJECT_CHUNK_SIZE_BYTES_KEY)) {
      optionsBuilder.setObjectChunkSizeBytes(
          Integer.parseInt(analyticsCoreOptions.get(prefix + OBJECT_CHUNK_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + OBJECT_CHUNK_MAX_FETCH_SPLITS_KEY)) {
      optionsBuilder.setObjectChunkMaxFetchSplits(
          Integer.parseInt(analyticsCoreOptions.get(prefix + OBJECT_CHUNK_MAX_FETCH_SPLITS_KEY)));
    }
    return optionsBuilder.build();
  }

  /** Builder for {@link GcsCacheOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets whether the Parquet footer cache is enabled. */
    public abstract Builder setFooterCacheEnabled(boolean footerCacheEnabled);

    /** Sets the maximum capacity (in bytes) to hold in the Parquet footer cache. */
    public abstract Builder setFooterCacheMaxSizeBytes(long footerCacheMaxSizeBytes);

    /** Sets the maximum capacity (in bytes) to hold in the small object cache. */
    /** Sets whether the small object cache is enabled. */
    public abstract Builder setSmallObjectCacheEnabled(boolean smallObjectCacheEnabled);

    /** Sets the maximum capacity (in bytes) to hold in the small object cache. */
    public abstract Builder setSmallObjectCacheMaxSizeBytes(long smallObjectCacheMaxSizeBytes);

    /** Sets whether the object-chunk cache is enabled. */
    public abstract Builder setObjectChunkCacheEnabled(boolean objectChunkCacheEnabled);

    /** Sets the maximum capacity (in bytes) to hold in the object-chunk cache. */
    public abstract Builder setObjectChunkCacheMaxSizeBytes(long objectChunkCacheMaxSizeBytes);

    /** Sets the chunk size (in bytes) the object is partitioned into for caching. */
    public abstract Builder setObjectChunkSizeBytes(int objectChunkSizeBytes);

    /**
     * Sets the maximum number of separate source requests a single read may issue to fetch its
     * missing chunks before falling back to a single whole-range request.
     */
    public abstract Builder setObjectChunkMaxFetchSplits(int objectChunkMaxFetchSplits);

    abstract GcsCacheOptions autoBuild();

    /**
     * Builds the {@link GcsCacheOptions} instance.
     *
     * @throws IllegalArgumentException if {@code footerCacheMaxSizeBytes} is non-positive when
     *     {@code footerCacheEnabled} is {@code true}.
     */
    public GcsCacheOptions build() {
      GcsCacheOptions options = autoBuild();
      if (options.isFooterCacheEnabled()) {
        checkArgument(
            options.getFooterCacheMaxSizeBytes() > 0,
            "footerCacheMaxSizeBytes must be positive when footerCacheEnabled is true");
      }
      if (options.isSmallObjectCacheEnabled()) {
        checkArgument(
            options.getSmallObjectCacheMaxSizeBytes() > 0,
            "smallObjectCacheMaxSizeBytes must be positive when smallObjectCacheEnabled is true");
      }
      if (options.isObjectChunkCacheEnabled()) {
        checkArgument(
            options.getObjectChunkCacheMaxSizeBytes() > 0,
            "objectChunkCacheMaxSizeBytes must be positive when objectChunkCacheEnabled is true");
        checkArgument(
            options.getObjectChunkSizeBytes() > 0,
            "objectChunkSizeBytes must be positive when objectChunkCacheEnabled is true");
        checkArgument(
            options.getObjectChunkMaxFetchSplits() > 0,
            "objectChunkMaxFetchSplits must be positive when objectChunkCacheEnabled is true");
      }
      return options;
    }
  }
}
