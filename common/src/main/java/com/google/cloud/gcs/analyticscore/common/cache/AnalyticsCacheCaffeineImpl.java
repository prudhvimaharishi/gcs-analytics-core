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

package com.google.cloud.gcs.analyticscore.common.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * An {@link AnalyticsCache} implementation backed by a Caffeine {@link Cache}. This implementation
 * is thread-safe.
 *
 * @param <K> The type of keys maintained by this cache.
 * @param <V> The type of mapped values.
 */
public class AnalyticsCacheCaffeineImpl<K, V> implements AnalyticsCache<K, V> {

  private final AsyncCache<K, V> asyncCache;

  private AnalyticsCacheCaffeineImpl(long maxWeight, Weigher<K, V> weigher) {
    checkArgument(maxWeight > 0, "maxWeight must be positive");
    checkNotNull(weigher, "weigher cannot be null");
    this.asyncCache = Caffeine.newBuilder().maximumWeight(maxWeight).weigher(weigher).buildAsync();
  }

  private AnalyticsCacheCaffeineImpl(long ttl, TimeUnit unit) {
    checkArgument(ttl > 0, "ttl must be positive");
    checkNotNull(unit, "unit cannot be null");
    this.asyncCache = Caffeine.newBuilder().expireAfterWrite(ttl, unit).buildAsync();
  }

  /**
   * Creates a new {@link AnalyticsCacheCaffeineImpl} with the specified maximum weight and weigher.
   */
  public static <K, V> AnalyticsCacheCaffeineImpl<K, V> create(
      long maxWeight, Weigher<K, V> weigher) {
    return new AnalyticsCacheCaffeineImpl<>(maxWeight, weigher);
  }

  /** Creates a new {@link AnalyticsCacheCaffeineImpl} with the specified time-to-live. */
  public static <K, V> AnalyticsCacheCaffeineImpl<K, V> createWithTtlOnly(long ttl, TimeUnit unit) {
    return new AnalyticsCacheCaffeineImpl<>(ttl, unit);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<V> get(K key) {
    checkNotNull(key, "key cannot be null");
    CompletableFuture<V> future = asyncCache.getIfPresent(key);
    if (future != null
        && future.isDone()
        && !future.isCompletedExceptionally()
        && !future.isCancelled()) {
      return Optional.ofNullable(future.join());
    }
    return Optional.empty();
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public <E extends Exception> V get(
      K key, ThrowingFunction<? super K, ? extends V, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    CompletableFuture<V> future = asyncCache.getIfPresent(key);
    if (future == null) {
      CompletableFuture<V> newFuture = new CompletableFuture<>();
      future = asyncCache.asMap().putIfAbsent(key, newFuture);
      if (future == null) {
        try {
          V computed = mappingFunction.apply(key);
          if (computed == null) {
            throw new NullPointerException("mappingFunction returned null for key: " + key);
          }
          newFuture.complete(computed);
        } catch (Throwable throwable) {
          asyncCache.asMap().remove(key, newFuture);
          newFuture.completeExceptionally(throwable);
        }
        future = newFuture;
      }
    }
    try {
      return future.join();
    } catch (CompletionException completionException) {
      Throwable cause = completionException.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      if (cause == null) {
        throw completionException;
      }
      throw (E) cause;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(K key, V value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    asyncCache.put(key, CompletableFuture.completedFuture(value));
  }

  /** {@inheritDoc} */
  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    asyncCache.synchronous().invalidate(key);
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateAll() {
    asyncCache.synchronous().invalidateAll();
  }

  /** {@inheritDoc} */
  @Override
  public long size() {
    return asyncCache.synchronous().estimatedSize();
  }

  /** {@inheritDoc} */
  @Override
  public void cleanUp() {
    asyncCache.synchronous().cleanUp();
  }
}
