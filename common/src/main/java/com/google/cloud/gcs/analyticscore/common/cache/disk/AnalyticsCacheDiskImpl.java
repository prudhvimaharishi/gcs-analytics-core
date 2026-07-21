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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.common.cache.AnalyticsCache;
import com.google.cloud.gcs.analyticscore.common.cache.ThrowingFunction;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

/**
 * An {@link AnalyticsCache} view over a worker-level {@link SharedDiskCache}, adapting the
 * String-keyed, host-shared engine to the typed cache interface so it can serve as an L2 tier
 * (typically under an in-memory L1 via {@code AnalyticsCacheHybridImpl}).
 *
 * <p>A {@code keyMapper} converts each typed key to the engine's String key. It may return {@link
 * Optional#empty()} to signal that a key is <b>not cacheable on disk</b> — for example a GCS object
 * whose content generation is unknown, which cannot be safely persisted because disk entries are
 * keyed by immutable generation. For such keys this cache transparently bypasses disk: lookups miss
 * and loads fall straight through to the mapping function.
 *
 * @param <K> The type of keys presented to this cache.
 */
public final class AnalyticsCacheDiskImpl<K> implements AnalyticsCache<K, ByteBuffer> {

  private final SharedDiskCache diskCache;
  private final Function<K, Optional<String>> keyMapper;

  /**
   * Creates a disk cache view.
   *
   * @param diskCache The shared, host-level engine backing this cache.
   * @param keyMapper Maps a typed key to its engine key, or {@link Optional#empty()} when the key
   *     must not be persisted on disk.
   */
  public static <K> AnalyticsCacheDiskImpl<K> create(
      SharedDiskCache diskCache, Function<K, Optional<String>> keyMapper) {
    return new AnalyticsCacheDiskImpl<>(diskCache, keyMapper);
  }

  private AnalyticsCacheDiskImpl(
      SharedDiskCache diskCache, Function<K, Optional<String>> keyMapper) {
    this.diskCache = checkNotNull(diskCache, "diskCache cannot be null");
    this.keyMapper = checkNotNull(keyMapper, "keyMapper cannot be null");
  }

  /** {@inheritDoc} */
  @Override
  public Optional<ByteBuffer> get(K key) {
    checkNotNull(key, "key cannot be null");
    return keyMapper.apply(key).flatMap(diskCache::read);
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> ByteBuffer get(
      K key, ThrowingFunction<? super K, ? extends ByteBuffer, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    Optional<String> diskKey = keyMapper.apply(key);
    if (!diskKey.isPresent()) {
      return requireNonNull(mappingFunction.apply(key), key);
    }
    return diskCache.getOrLoad(
        diskKey.get(), () -> requireNonNull(mappingFunction.apply(key), key));
  }

  /** {@inheritDoc} */
  @Override
  public void put(K key, ByteBuffer value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    keyMapper
        .apply(key)
        .ifPresent(
            diskKey -> {
              ByteBuffer contents = value.duplicate();
              contents.rewind();
              diskCache.write(diskKey, contents);
            });
  }

  /** {@inheritDoc} */
  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    keyMapper.apply(key).ifPresent(diskCache::invalidate);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This is intentionally a no-op: the disk tier is shared with other executor processes on the
   * host, so a single process must not wipe it. Entries are reclaimed only by TTL expiry and the
   * background eviction janitor.
   */
  @Override
  public void invalidateAll() {
    // Shared across processes; never cleared wholesale from a single JVM.
  }

  /** {@inheritDoc} Returns {@code 0}: the on-disk entry count is not tracked on the hot path. */
  @Override
  public long size() {
    return 0;
  }

  private static ByteBuffer requireNonNull(ByteBuffer value, Object key) {
    if (value == null) {
      throw new NullPointerException("mappingFunction returned null for key: " + key);
    }
    return value;
  }
}
