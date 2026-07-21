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

package com.google.cloud.gcs.analyticscore.common.cache.disk;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

/** Configuration options for a {@link SharedDiskCache}. */
@AutoValue
public abstract class SharedDiskCacheOptions {

  private static final double DEFAULT_EVICTION_HIGH_WATERMARK = 0.95;
  private static final double DEFAULT_EVICTION_LOW_WATERMARK = 0.85;
  private static final long DEFAULT_MAINTENANCE_PERIOD_MILLIS = 60_000;

  /** Returns the root directory of the cache, shared by all processes on the host. */
  public abstract String getCacheDirectory();

  /** Returns the maximum total size (in bytes) of all cache entries under the root directory. */
  public abstract long getMaxSizeBytes();

  /**
   * Returns the time-to-live (in milliseconds) of a cache entry, measured from its creation time. A
   * value of {@code 0} disables TTL-based expiry.
   */
  public abstract long getTtlMillis();

  /**
   * Returns the usage fraction of {@link #getMaxSizeBytes()} above which the janitor starts
   * evicting entries.
   */
  public abstract double getEvictionHighWatermark();

  /**
   * Returns the usage fraction of {@link #getMaxSizeBytes()} down to which the janitor evicts
   * entries once eviction starts.
   */
  public abstract double getEvictionLowWatermark();

  /** Returns the period (in milliseconds) between background maintenance runs. */
  public abstract long getMaintenancePeriodMillis();

  /** Returns a new builder for {@link SharedDiskCacheOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_SharedDiskCacheOptions.Builder()
        .setTtlMillis(0)
        .setEvictionHighWatermark(DEFAULT_EVICTION_HIGH_WATERMARK)
        .setEvictionLowWatermark(DEFAULT_EVICTION_LOW_WATERMARK)
        .setMaintenancePeriodMillis(DEFAULT_MAINTENANCE_PERIOD_MILLIS);
  }

  /** Builder for {@link SharedDiskCacheOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the root directory of the cache. */
    public abstract Builder setCacheDirectory(String cacheDirectory);

    /** Sets the maximum total size (in bytes) of all cache entries. */
    public abstract Builder setMaxSizeBytes(long maxSizeBytes);

    /** Sets the time-to-live (in milliseconds) of a cache entry; {@code 0} disables TTL. */
    public abstract Builder setTtlMillis(long ttlMillis);

    /** Sets the usage fraction above which the janitor starts evicting entries. */
    public abstract Builder setEvictionHighWatermark(double evictionHighWatermark);

    /** Sets the usage fraction down to which the janitor evicts entries. */
    public abstract Builder setEvictionLowWatermark(double evictionLowWatermark);

    /** Sets the period (in milliseconds) between background maintenance runs. */
    public abstract Builder setMaintenancePeriodMillis(long maintenancePeriodMillis);

    abstract SharedDiskCacheOptions autoBuild();

    /**
     * Builds the {@link SharedDiskCacheOptions} instance.
     *
     * @throws IllegalArgumentException if any option is out of range.
     */
    public SharedDiskCacheOptions build() {
      SharedDiskCacheOptions options = autoBuild();
      checkArgument(!options.getCacheDirectory().isEmpty(), "cacheDirectory cannot be empty");
      checkArgument(options.getMaxSizeBytes() > 0, "maxSizeBytes must be positive");
      checkArgument(options.getTtlMillis() >= 0, "ttlMillis cannot be negative");
      checkArgument(
          options.getMaintenancePeriodMillis() > 0, "maintenancePeriodMillis must be positive");
      checkArgument(
          options.getEvictionLowWatermark() > 0
              && options.getEvictionLowWatermark() < options.getEvictionHighWatermark()
              && options.getEvictionHighWatermark() <= 1.0,
          "watermarks must satisfy 0 < lowWatermark < highWatermark <= 1, got low=%s high=%s",
          options.getEvictionLowWatermark(),
          options.getEvictionHighWatermark());
      return options;
    }
  }
}
