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

package com.google.cloud.gcs.analyticscore.common.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

public class AnalyticsCacheHybridImpl<K, V> implements AnalyticsCache<K, V> {

  private final AnalyticsCache<K, V> l1Cache;
  private final AnalyticsCache<K, V> l2Cache;

  public static <K, V> AnalyticsCacheHybridImpl<K, V> create(
      AnalyticsCache<K, V> l1Cache, AnalyticsCache<K, V> l2Cache) {
    return new AnalyticsCacheHybridImpl<>(l1Cache, l2Cache);
  }

  @Override
  public Optional<V> get(K key) {
    checkNotNull(key, "key cannot be null");
    Optional<V> value = l1Cache.get(key);
    if (value.isPresent()) {
      return value;
    }
    value = l2Cache.get(key);
    value.ifPresent(v -> l1Cache.put(key, v));
    return value;
  }

  @Override
  public <E extends Exception> V get(
      K key, ThrowingFunction<? super K, ? extends V, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    return l1Cache.get(key, k -> l2Cache.get(k, mappingFunction));
  }

  @Override
  public void put(K key, V value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    l1Cache.put(key, value);
    l2Cache.put(key, value);
  }

  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    l1Cache.invalidate(key);
    l2Cache.invalidate(key);
  }

  @Override
  public void invalidateAll() {
    l1Cache.invalidateAll();
    l2Cache.invalidateAll();
  }

  @Override
  public long size() {
    return l1Cache.size();
  }

  @Override
  public void cleanUp() {
    l1Cache.cleanUp();
    l2Cache.cleanUp();
  }

  private AnalyticsCacheHybridImpl(AnalyticsCache<K, V> l1Cache, AnalyticsCache<K, V> l2Cache) {
    this.l1Cache = checkNotNull(l1Cache, "l1Cache cannot be null");
    this.l2Cache = checkNotNull(l2Cache, "l2Cache cannot be null");
  }
}
