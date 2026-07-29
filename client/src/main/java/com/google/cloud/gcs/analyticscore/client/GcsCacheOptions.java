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
import java.util.Locale;
import java.util.Map;

/** Configuration options for the GCS caching layer. */
@AutoValue
public abstract class GcsCacheOptions {

  static final String FOOTER_CACHE_ENABLED_KEY = "analytics-core.footer.cache.enabled";
  static final String FOOTER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.footer.cache.max-size-bytes";
  static final String SMALL_FILE_CACHE_ENABLED_KEY = "analytics-core.small-file.cache.enabled";
  static final String SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.small-file.cache.max-size-bytes";
  static final String CACHE_SCOPE_KEY = "analytics-core.cache.scope";
  static final String UNIFORM_BUCKET_LEVEL_ACCESS_ENABLED_KEY =
      "analytics-core.cache.uniform-bucket-level-access.enabled";

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = false;
  private static final long DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES = 1024 * MB;
  private static final boolean DEFAULT_SMALL_OBJECT_CACHE_ENABLED = false;
  private static final long DEFAULT_SMALL_OBJECT_CACHE_MAX_SIZE_BYTES = 1024 * MB;
  private static final GcsCacheScope DEFAULT_CACHE_SCOPE = GcsCacheScope.INSTANCE;
  private static final boolean DEFAULT_UNIFORM_BUCKET_LEVEL_ACCESS_ENABLED = false;

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the Parquet footer cache. */
  public abstract long getFooterCacheMaxSizeBytes();

  /** Returns whether the small object cache is enabled. */
  public abstract boolean isSmallObjectCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the small object cache. */
  public abstract long getSmallObjectCacheMaxSizeBytes();

  /** Returns the sharing scope of the caching layer. */
  public abstract GcsCacheScope getCacheScope();

  /**
   * Returns whether Uniform Bucket-Level Access (UBLA) fast-path caching is enabled.
   *
   * <p>Enabling this flag indicates that the user agrees that target buckets use Uniform
   * Bucket-Level Access (UBLA) with uniform IAM permissions across prefix objects, and that no
   * principals accessing the bucket have object-level ACLs configured.
   */
  public abstract boolean isUniformBucketLevelAccessEnabled();

  /** Resolves the effective {@link GcsCachingMode} based on the configuration flags. */
  public GcsCachingMode resolveCachingMode() {
    switch (getCacheScope()) {
      case INSTANCE:
        return GcsCachingMode.PER_INSTANCE;
      case EXECUTOR:
        return isUniformBucketLevelAccessEnabled()
            ? GcsCachingMode.SHARED_GLOBAL
            : GcsCachingMode.SHARED_PER_CREDENTIAL;
      default:
        throw new IllegalStateException("Unknown cache scope: " + getCacheScope());
    }
  }

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
        .setCacheScope(DEFAULT_CACHE_SCOPE)
        .setUniformBucketLevelAccessEnabled(DEFAULT_UNIFORM_BUCKET_LEVEL_ACCESS_ENABLED);
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
    if (analyticsCoreOptions.containsKey(prefix + CACHE_SCOPE_KEY)) {
      optionsBuilder.setCacheScope(
          GcsCacheScope.valueOf(
              analyticsCoreOptions.get(prefix + CACHE_SCOPE_KEY).toUpperCase(Locale.US)));
    }
    if (analyticsCoreOptions.containsKey(prefix + UNIFORM_BUCKET_LEVEL_ACCESS_ENABLED_KEY)) {
      optionsBuilder.setUniformBucketLevelAccessEnabled(
          Boolean.parseBoolean(
              analyticsCoreOptions.get(prefix + UNIFORM_BUCKET_LEVEL_ACCESS_ENABLED_KEY)));
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

    /** Sets the sharing scope of the caching layer. */
    public abstract Builder setCacheScope(GcsCacheScope cacheScope);

    /** Sets whether Uniform Bucket-Level Access (UBLA) fast-path caching is enabled. */
    public abstract Builder setUniformBucketLevelAccessEnabled(
        boolean uniformBucketLevelAccessEnabled);

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

      return options;
    }
  }
}
