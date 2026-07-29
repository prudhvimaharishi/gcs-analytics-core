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

import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.Weigher;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheCaffeineImpl;
import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCacheNoOpImpl;
import com.google.cloud.gcs.analyticscore.common.cache.ThrowingFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Manages the caching layer for GCS objects. This class is thread-safe and acts as a registry for
 * various specialized caches (e.g., Parquet footer cache).
 */
public class AnalyticsCacheManager {

  /**
   * TTL for bucket properties cache in minutes. A 10-minute TTL is used because a bucket's
   * Hierarchical Namespace configuration is immutable unless the bucket is deleted and re-created.
   */
  private static final long BUCKET_PROPERTIES_CACHE_TTL_MINUTES = 10;

  private static volatile AnalyticsCache<ScopedGcsItemId, ByteBuffer> footerCache;
  private static volatile AnalyticsCache<ScopedGcsItemId, ByteBuffer> smallObjectCache;
  private final AnalyticsCache<String, BucketProperties> bucketPropertiesCache;
  private final Set<String> authorizedPrefixes = ConcurrentHashMap.newKeySet();
  private final String activeScopeId;
  private final GcsCachingMode cachingMode;

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified options and default credentials.
   *
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(GcsCacheOptions options) {
    this(null, options);
  }

  /**
   * Creates a new {@link AnalyticsCacheManager} with the specified credentials and options.
   *
   * @param credentials The GCP credentials used to derive an access boundary scope.
   * @param options The configuration options for the caching layer.
   */
  public AnalyticsCacheManager(@Nullable Credentials credentials, GcsCacheOptions options) {
    checkNotNull(options, "options cannot be null");
    this.cachingMode = options.resolveCachingMode();
    if (cachingMode == GcsCachingMode.PER_INSTANCE) {
      this.activeScopeId = "fs:" + UUID.randomUUID();
    } else if (cachingMode == GcsCachingMode.SHARED_GLOBAL) {
      this.activeScopeId = "global";
    } else {
      this.activeScopeId = extractScope(credentials);
    }

    Weigher<ScopedGcsItemId, ByteBuffer> weigher = (key, value) -> value.remaining();
    if (footerCache == null || smallObjectCache == null) {
      synchronized (AnalyticsCacheManager.class) {
        if (footerCache == null) {
          footerCache =
              options.isFooterCacheEnabled()
                  ? AnalyticsCacheCaffeineImpl.create(options.getFooterCacheMaxSizeBytes(), weigher)
                  : AnalyticsCacheNoOpImpl.getInstance();
        }
        if (smallObjectCache == null) {
          smallObjectCache =
              options.isSmallObjectCacheEnabled()
                  ? AnalyticsCacheCaffeineImpl.create(
                      options.getSmallObjectCacheMaxSizeBytes(), weigher)
                  : AnalyticsCacheNoOpImpl.getInstance();
        }
      }
    }
    this.bucketPropertiesCache =
        AnalyticsCacheCaffeineImpl.createWithTtlOnly(
            BUCKET_PROPERTIES_CACHE_TTL_MINUTES, TimeUnit.MINUTES);
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
    return getOrLoad(itemId, footerCache, footerLoader::load);
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
    return getOrLoad(itemId, smallObjectCache, smallObjectLoader::load);
  }

  /** Invalidates the cached footer for the given {@code itemId}. */
  public void invalidateFooter(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    if (footerCache != null) {
      footerCache.invalidate(ScopedGcsItemId.create(activeScopeId, itemId));
    }
  }

  /** Invalidates the cached small object for the given {@code itemId}. */
  public void invalidateSmallObject(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    if (smallObjectCache != null) {
      smallObjectCache.invalidate(ScopedGcsItemId.create(activeScopeId, itemId));
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

  /** Invalidates all cached entries. */
  public void invalidateAll() {
    if (footerCache != null) {
      footerCache.invalidateAll();
    }
    if (smallObjectCache != null) {
      smallObjectCache.invalidateAll();
    }
    bucketPropertiesCache.invalidateAll();
    authorizedPrefixes.clear();
  }

  @VisibleForTesting
  static synchronized void resetCaches() {
    if (footerCache != null) {
      footerCache.invalidateAll();
      footerCache = null;
    }
    if (smallObjectCache != null) {
      smallObjectCache.invalidateAll();
      smallObjectCache = null;
    }
  }

  @VisibleForTesting
  static String extractScope(@Nullable Credentials credentials) {
    if (credentials == null) {
      return "adc";
    }
    if (credentials instanceof NoCredentials) {
      return "anon";
    }
    if (credentials instanceof ImpersonatedCredentials) {
      return "imp:" + ((ImpersonatedCredentials) credentials).getAccount();
    }
    if (credentials instanceof ServiceAccountCredentials) {
      return "sa:" + ((ServiceAccountCredentials) credentials).getClientEmail();
    }
    if (credentials instanceof OAuth2Credentials) {
      AccessToken token = ((OAuth2Credentials) credentials).getAccessToken();
      if (token != null && token.getTokenValue() != null) {
        return "tok:"
            + Hashing.sha256().hashString(token.getTokenValue(), StandardCharsets.UTF_8).toString();
      }
    }
    return "cred:" + Integer.toHexString(System.identityHashCode(credentials));
  }

  private ByteBuffer getOrLoad(
      GcsItemId itemId,
      @Nullable AnalyticsCache<ScopedGcsItemId, ByteBuffer> cache,
      ThrowingFunction<GcsItemId, ByteBuffer, IOException> loader)
      throws IOException {
    if (cache == null) {
      return loader.apply(itemId).asReadOnlyBuffer();
    }

    ScopedGcsItemId scopedId = ScopedGcsItemId.create(activeScopeId, itemId);
    // For uniform access mode, prove read access to the prefix once before using shared cache
    if (cachingMode == GcsCachingMode.SHARED_GLOBAL) {
      String prefix = getParentPrefix(itemId);
      if (!authorizedPrefixes.contains(prefix)) {
        ByteBuffer loadedObject = loader.apply(itemId);
        authorizedPrefixes.add(prefix);
        cache.put(scopedId, loadedObject);
        return loadedObject.asReadOnlyBuffer();
      }
    }

    return cache.get(scopedId, cachedId -> loader.apply(cachedId.getItemId())).asReadOnlyBuffer();
  }

  private static String getParentPrefix(GcsItemId itemId) {
    String bucketName = itemId.getBucketName();
    String objectName = itemId.getObjectName().orElse("");
    int lastSlashIndex = objectName.lastIndexOf('/');
    if (lastSlashIndex >= 0) {
      return bucketName + "/" + objectName.substring(0, lastSlashIndex + 1);
    }
    return bucketName + "/" + objectName;
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
