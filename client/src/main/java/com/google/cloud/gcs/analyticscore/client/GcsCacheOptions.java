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
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Configuration options for the GCS caching layer. */
@AutoValue
public abstract class GcsCacheOptions {

  static final String FOOTER_CACHE_ENABLED_KEY = "analytics-core.footer.cache.enabled";
  static final String FOOTER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.footer.cache.max-size-bytes";
  static final String SMALL_FILE_CACHE_ENABLED_KEY = "analytics-core.small-file.cache.enabled";
  static final String SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.small-file.cache.max-size-bytes";
  static final String WORKER_CACHE_ENABLED_KEY = "analytics-core.worker.cache.enabled";
  static final String WORKER_CACHE_DIRECTORY_KEY = "analytics-core.worker.cache.directory";
  static final String WORKER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.worker.cache.max-size-bytes";
  static final String WORKER_CACHE_TTL_MILLIS_KEY = "analytics-core.worker.cache.ttl-millis";
  static final String WORKER_CACHE_HIGH_WATERMARK_KEY =
      "analytics-core.worker.cache.eviction.high-watermark";
  static final String WORKER_CACHE_LOW_WATERMARK_KEY =
      "analytics-core.worker.cache.eviction.low-watermark";

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = false;
  private static final long DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES = 1024 * MB;
  private static final boolean DEFAULT_SMALL_OBJECT_CACHE_ENABLED = false;
  private static final long DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES = 1024 * MB;
  private static final boolean DEFAULT_WORKER_CACHE_ENABLED = false;
  private static final String DEFAULT_WORKER_CACHE_DIRECTORY =
      Paths.get(System.getProperty("java.io.tmpdir", "/tmp"), "gcs-analytics-cache").toString();
  private static final long DEFAULT_WORKER_CACHE_MAX_SIZE_BYTES = 10240 * MB;
  private static final long DEFAULT_WORKER_CACHE_TTL_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final double DEFAULT_WORKER_CACHE_HIGH_WATERMARK = 0.95;
  private static final double DEFAULT_WORKER_CACHE_LOW_WATERMARK = 0.85;

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the Parquet footer cache. */
  public abstract long getFooterCacheMaxSizeBytes();

  /** Returns whether the small object cache is enabled. */
  public abstract boolean isSmallObjectCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the small object cache. */
  public abstract long getSmallObjectCacheMaxSizeBytes();

  /** Returns whether the worker-level file cache is enabled. */
  public abstract boolean isWorkerCacheEnabled();

  /** Returns the directory for the worker-level file cache. */
  public abstract String getWorkerCacheDirectory();

  /** Returns the maximum capacity (in bytes) to hold in the worker-level file cache. */
  public abstract long getWorkerCacheMaxSizeBytes();

  /**
   * Returns the time-to-live (in milliseconds) of a worker cache entry, measured from creation.
   * {@code 0} disables TTL-based expiry. Entries are keyed by object generation, so TTL bounds disk
   * turnover rather than correctness.
   */
  public abstract long getWorkerCacheTtlMillis();

  /** Returns the disk usage fraction above which background LRU eviction starts. */
  public abstract double getWorkerCacheEvictionHighWatermark();

  /** Returns the disk usage fraction down to which background LRU eviction proceeds. */
  public abstract double getWorkerCacheEvictionLowWatermark();

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
        .setWorkerCacheEnabled(DEFAULT_WORKER_CACHE_ENABLED)
        .setWorkerCacheDirectory(DEFAULT_WORKER_CACHE_DIRECTORY)
        .setWorkerCacheMaxSizeBytes(DEFAULT_WORKER_CACHE_MAX_SIZE_BYTES)
        .setWorkerCacheTtlMillis(DEFAULT_WORKER_CACHE_TTL_MILLIS)
        .setWorkerCacheEvictionHighWatermark(DEFAULT_WORKER_CACHE_HIGH_WATERMARK)
        .setWorkerCacheEvictionLowWatermark(DEFAULT_WORKER_CACHE_LOW_WATERMARK);
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
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_ENABLED_KEY)) {
      optionsBuilder.setWorkerCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + WORKER_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_DIRECTORY_KEY)) {
      optionsBuilder.setWorkerCacheDirectory(
          analyticsCoreOptions.get(prefix + WORKER_CACHE_DIRECTORY_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setWorkerCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + WORKER_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_TTL_MILLIS_KEY)) {
      optionsBuilder.setWorkerCacheTtlMillis(
          Long.parseLong(analyticsCoreOptions.get(prefix + WORKER_CACHE_TTL_MILLIS_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_HIGH_WATERMARK_KEY)) {
      optionsBuilder.setWorkerCacheEvictionHighWatermark(
          Double.parseDouble(analyticsCoreOptions.get(prefix + WORKER_CACHE_HIGH_WATERMARK_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + WORKER_CACHE_LOW_WATERMARK_KEY)) {
      optionsBuilder.setWorkerCacheEvictionLowWatermark(
          Double.parseDouble(analyticsCoreOptions.get(prefix + WORKER_CACHE_LOW_WATERMARK_KEY)));
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

    /** Sets whether the small object cache is enabled. */
    public abstract Builder setSmallObjectCacheEnabled(boolean smallObjectCacheEnabled);

    /** Sets the maximum capacity (in bytes) to hold in the small object cache. */
    public abstract Builder setSmallObjectCacheMaxSizeBytes(long smallObjectCacheMaxSizeBytes);

    /** Sets whether the worker-level file cache is enabled. */
    public abstract Builder setWorkerCacheEnabled(boolean workerCacheEnabled);

    /** Sets the directory for the worker-level file cache. */
    public abstract Builder setWorkerCacheDirectory(String workerCacheDirectory);

    /** Sets the maximum capacity (in bytes) to hold in the worker-level file cache. */
    public abstract Builder setWorkerCacheMaxSizeBytes(long workerCacheMaxSizeBytes);

    /** Sets the time-to-live (in milliseconds) of a worker cache entry; {@code 0} disables TTL. */
    public abstract Builder setWorkerCacheTtlMillis(long workerCacheTtlMillis);

    /** Sets the disk usage fraction above which background eviction starts. */
    public abstract Builder setWorkerCacheEvictionHighWatermark(
        double workerCacheEvictionHighWatermark);

    /** Sets the disk usage fraction down to which background eviction proceeds. */
    public abstract Builder setWorkerCacheEvictionLowWatermark(
        double workerCacheEvictionLowWatermark);

    abstract GcsCacheOptions autoBuild();

    /**
     * Builds the {@link GcsCacheOptions} instance.
     *
     * @throws IllegalArgumentException if options are invalid.
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
      if (options.isWorkerCacheEnabled()) {
        checkArgument(
            options.getWorkerCacheMaxSizeBytes() > 0,
            "workerCacheMaxSizeBytes must be positive when workerCacheEnabled is true");
        checkArgument(
            options.getWorkerCacheDirectory() != null
                && !options.getWorkerCacheDirectory().trim().isEmpty(),
            "workerCacheDirectory must not be null or empty when workerCacheEnabled is true");
        checkArgument(
            options.getWorkerCacheTtlMillis() >= 0, "workerCacheTtlMillis cannot be negative");
        checkArgument(
            options.getWorkerCacheEvictionLowWatermark() > 0
                && options.getWorkerCacheEvictionLowWatermark()
                    < options.getWorkerCacheEvictionHighWatermark()
                && options.getWorkerCacheEvictionHighWatermark() <= 1.0,
            "worker cache watermarks must satisfy 0 < lowWatermark < highWatermark <= 1");
      }

      return options;
    }
  }
}
