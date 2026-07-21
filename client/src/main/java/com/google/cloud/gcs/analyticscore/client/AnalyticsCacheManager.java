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

import com.github.benmanes.caffeine.cache.Weigher;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheHybridImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import com.google.cloud.gcs.analyticscore.common.cache.disk.AnalyticsCacheDiskImpl;
import com.google.cloud.gcs.analyticscore.common.cache.disk.SharedDiskCache;
import com.google.cloud.gcs.analyticscore.common.cache.disk.SharedDiskCacheOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the caching layer for GCS objects. This class is thread-safe and acts as a registry for
 * various specialized caches (e.g., Parquet footer cache).
 *
 * <p>The footer and small object caches are held in JVM-wide static fields so they are shared by
 * every {@link GcsFileSystem} instance in an executor process. When the worker-level cache is
 * enabled, each in-memory cache becomes the L1 tier of an {@link AnalyticsCacheHybridImpl} whose L2
 * tier is a {@link SharedDiskCache} shared by every executor process on the host. A single disk
 * engine (one directory, one size budget, one eviction janitor) backs both the footer and small
 * object tiers; entries are distinguished by a key prefix. Only objects with a known content
 * generation are persisted to disk, because GCS content is immutable per generation and such
 * entries can never be stale.
 */
public class AnalyticsCacheManager {

  private static final Logger LOG = LoggerFactory.getLogger(AnalyticsCacheManager.class);

  /**
   * TTL for bucket properties cache in minutes. A 10-minute TTL is used because a bucket's
   * Hierarchical Namespace configuration is immutable unless the bucket is deleted and re-created.
   */
  private static final long BUCKET_PROPERTIES_CACHE_TTL_MINUTES = 10;

  private static final String FOOTER_DISK_ENTRY_KIND = "footer";
  private static final String SMALL_OBJECT_DISK_ENTRY_KIND = "small-object";

  private static volatile AnalyticsCache<GcsItemId, ByteBuffer> footerCache;
  private static volatile AnalyticsCache<GcsItemId, ByteBuffer> smallObjectCache;
  private final AnalyticsCache<String, BucketProperties> bucketPropertiesCache;

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options and no telemetry
   * reporting.
   *
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(GcsCacheOptions options) {
    this(options, new Telemetry(ImmutableList.of()));
  }

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options.
   *
   * @param options The configuration options for the caching layer.
   * @param telemetry The telemetry used to report cache metrics.
   */
  public AnalyticsCacheManager(GcsCacheOptions options, Telemetry telemetry) {
    checkNotNull(options, "options cannot be null");
    checkNotNull(telemetry, "telemetry cannot be null");
    Weigher<GcsItemId, ByteBuffer> weigher = (key, value) -> value.remaining();
    if (footerCache == null || smallObjectCache == null) {
      synchronized (AnalyticsCacheManager.class) {
        Optional<SharedDiskCache> diskCache = createDiskCache(options, telemetry);
        if (footerCache == null) {
          footerCache =
              createCache(
                  options.isFooterCacheEnabled(),
                  options.getFooterCacheMaxSizeBytes(),
                  weigher,
                  diskCache,
                  FOOTER_DISK_ENTRY_KIND);
        }
        if (smallObjectCache == null) {
          smallObjectCache =
              createCache(
                  options.isSmallObjectCacheEnabled(),
                  options.getSmallObjectCacheMaxSizeBytes(),
                  weigher,
                  diskCache,
                  SMALL_OBJECT_DISK_ENTRY_KIND);
        }
      }
    }
    this.bucketPropertiesCache =
        AnalyticsCacheCaffeineImpl.createWithTtlOnly(
            BUCKET_PROPERTIES_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
  }

  /**
   * Composes the caching tiers for one cache kind: an in-memory L1 (when the memory cache is
   * enabled) over a shared disk L2 (when the worker cache is available). Returns a no-op cache when
   * neither tier is active.
   */
  private static AnalyticsCache<GcsItemId, ByteBuffer> createCache(
      boolean memoryCacheEnabled,
      long memoryCacheMaxSizeBytes,
      Weigher<GcsItemId, ByteBuffer> weigher,
      Optional<SharedDiskCache> diskCache,
      String entryKind) {
    AnalyticsCache<GcsItemId, ByteBuffer> l1Cache =
        memoryCacheEnabled
            ? AnalyticsCacheCaffeineImpl.create(memoryCacheMaxSizeBytes, weigher)
            : null;
    AnalyticsCache<GcsItemId, ByteBuffer> l2Cache =
        diskCache
            .<AnalyticsCache<GcsItemId, ByteBuffer>>map(
                engine ->
                    AnalyticsCacheDiskImpl.create(
                        engine, itemId -> diskEntryKey(itemId, entryKind)))
            .orElse(null);

    if (l1Cache != null && l2Cache != null) {
      return AnalyticsCacheHybridImpl.create(l1Cache, l2Cache);
    }
    if (l1Cache != null) {
      return l1Cache;
    }
    if (l2Cache != null) {
      return l2Cache;
    }
    return AnalyticsCacheNoOpImpl.getInstance();
  }

  private static Optional<SharedDiskCache> createDiskCache(
      GcsCacheOptions options, Telemetry telemetry) {
    if (!options.isWorkerCacheEnabled()) {
      return Optional.empty();
    }
    try {
      SharedDiskCacheOptions diskCacheOptions =
          SharedDiskCacheOptions.builder()
              .setCacheDirectory(options.getWorkerCacheDirectory())
              .setMaxSizeBytes(options.getWorkerCacheMaxSizeBytes())
              .setTtlMillis(options.getWorkerCacheTtlMillis())
              .setEvictionHighWatermark(options.getWorkerCacheEvictionHighWatermark())
              .setEvictionLowWatermark(options.getWorkerCacheEvictionLowWatermark())
              .build();
      return Optional.of(SharedDiskCache.getOrCreate(diskCacheOptions, telemetry));
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to initialize the worker cache; continuing without it", e);
      return Optional.empty();
    }
  }

  /**
   * Returns the disk entry key for {@code itemId}, or {@link Optional#empty()} when the object's
   * name is not present. The {@code entryKind} prefix keeps footer and small object entries
   * distinct within the shared engine.
   */
  private static Optional<String> diskEntryKey(GcsItemId itemId, String entryKind) {
    return itemId
        .getObjectName()
        .map(objectName -> entryKind + '/' + itemId.getBucketName() + '/' + objectName);
  }

  /**
   * Returns the cached footer for the given {@code itemId}, obtaining it from the {@code
   * footerLoader} if necessary. This method is atomic; the {@code footerLoader} will be applied at
   * most once per itemId during concurrent access.
   *
   * <p>If the {@code footerLoader} throws an exception, it will be propagated to the caller and the
   * result will not be cached.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getFooter(GcsItemId itemId, FooterLoader footerLoader) throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(footerLoader, "footerLoader cannot be null");

    return footerCache.get(itemId, footerLoader::load).asReadOnlyBuffer();
  }

  /**
   * Returns the cached small object for the given {@code itemId}, obtaining it from the {@code
   * smallObjectLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public ByteBuffer getSmallObject(GcsItemId itemId, SmallObjectLoader smallObjectLoader)
      throws IOException {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(smallObjectLoader, "smallObjectLoader cannot be null");

    return smallObjectCache.get(itemId, smallObjectLoader::load).asReadOnlyBuffer();
  }

  /** Invalidates the cached footer for the given {@code itemId} in every tier. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    if (footerCache != null) {
      footerCache.invalidate(itemId);
    }
  }

  /** Invalidates the cached small object for the given {@code itemId} in every tier. */
  public void invalidateSmallObject(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    if (smallObjectCache != null) {
      smallObjectCache.invalidate(itemId);
    }
  }

  /**
   * Returns the cached properties for the given {@code bucketName}, obtaining it from the {@code
   * bucketPropertiesLoader} if necessary. This method is atomic.
   *
   * @throws IOException if the loader throws an {@link IOException}.
   */
  public BucketProperties getBucketProperties(
      String bucketName, BucketPropertiesLoader bucketPropertiesLoader) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    checkNotNull(bucketPropertiesLoader, "bucketPropertiesLoader cannot be null");

    return bucketPropertiesCache.get(bucketName, bucketPropertiesLoader::load);
  }

  /** Invalidates the cached properties for the given {@code bucketName}. */
  public void invalidateBucketProperties(String bucketName) {
    checkNotNull(bucketName, "bucketName cannot be null");
    bucketPropertiesCache.invalidate(bucketName);
  }

  /**
   * Invalidates all in-memory cached entries. The worker-level disk tier is left untouched: it is
   * shared with other executor processes on the host, and its generation-keyed entries cannot be
   * stale (see {@link AnalyticsCacheDiskImpl#invalidateAll()}).
   */
  public void invalidateAll() {
    if (footerCache != null) {
      footerCache.invalidateAll();
    }
    if (smallObjectCache != null) {
      smallObjectCache.invalidateAll();
    }
    bucketPropertiesCache.invalidateAll();
  }

  /** Clears the JVM-wide static caches so tests can start from a clean slate. */
  @VisibleForTesting
  public static synchronized void resetCaches() {
    if (footerCache != null) {
      footerCache.invalidateAll();
      footerCache = null;
    }
    if (smallObjectCache != null) {
      smallObjectCache.invalidateAll();
      smallObjectCache = null;
    }
  }

  /** Drops the JVM-wide static cache references without invalidating their entries. */
  @VisibleForTesting
  public static synchronized void clearStaticCacheReferences() {
    footerCache = null;
    smallObjectCache = null;
  }

  /** A loader for GCS object footers. */
  @FunctionalInterface
  public interface FooterLoader {
    /** Loads the footer for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for small GCS objects. */
  @FunctionalInterface
  public interface SmallObjectLoader {
    /** Loads the small object for the given {@code itemId}. */
    ByteBuffer load(GcsItemId itemId) throws IOException;
  }

  /** A loader for GCS bucket properties. */
  @FunctionalInterface
  public interface BucketPropertiesLoader {
    /** Loads the properties for the given {@code bucketName}. */
    BucketProperties load(String bucketName) throws IOException;
  }
}
