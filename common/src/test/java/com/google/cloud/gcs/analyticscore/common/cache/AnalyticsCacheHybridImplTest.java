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

import static com.google.common.truth.Truth.assertThat;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheHybridImplTest {

  private AnalyticsCacheCaffeineImpl<String, ByteBuffer> l1Cache;
  private AnalyticsCacheCaffeineImpl<String, ByteBuffer> l2Cache;
  private AnalyticsCacheHybridImpl<String, ByteBuffer> hybridCache;

  @BeforeEach
  void setUp() {
    l1Cache = AnalyticsCacheCaffeineImpl.create(1024, (k, v) -> v.remaining());
    l2Cache = AnalyticsCacheCaffeineImpl.create(1024, (k, v) -> v.remaining());
    hybridCache = AnalyticsCacheHybridImpl.create(l1Cache, l2Cache);
  }

  @Test
  void get_presentInL1_returnsFromL1() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    l1Cache.put(key, buffer);

    Optional<ByteBuffer> result = hybridCache.get(key);

    assertThat(result).hasValue(buffer);
  }

  @Test
  void get_presentInL2_populatesL1AndReturns() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    l2Cache.put(key, buffer);

    Optional<ByteBuffer> result = hybridCache.get(key);

    assertThat(result).hasValue(buffer);
    assertThat(l1Cache.get(key)).hasValue(buffer);
  }

  @Test
  void get_withMappingFunction_absentInBoth_computesAndPopulatesBoth() throws Exception {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer result =
        hybridCache.get(
            key,
            k -> {
              callCount.incrementAndGet();
              return buffer;
            });

    assertThat(result).isEqualTo(buffer);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(l1Cache.get(key)).hasValue(buffer);
    assertThat(l2Cache.get(key)).hasValue(buffer);
  }

  @Test
  void put_anyKeyValue_putsInBoth() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});

    hybridCache.put(key, buffer);

    assertThat(l1Cache.get(key)).hasValue(buffer);
    assertThat(l2Cache.get(key)).hasValue(buffer);
  }

  @Test
  void invalidate_presentInBoth_removesFromBoth() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    hybridCache.put(key, buffer);

    hybridCache.invalidate(key);

    assertThat(l1Cache.get(key)).isEmpty();
    assertThat(l2Cache.get(key)).isEmpty();
  }

  @Test
  void invalidateAll_withEntries_clearsBoth() {
    hybridCache.put("key1", ByteBuffer.wrap(new byte[] {1}));
    hybridCache.put("key2", ByteBuffer.wrap(new byte[] {2}));

    hybridCache.invalidateAll();

    assertThat(l1Cache.get("key1")).isEmpty();
    assertThat(l2Cache.get("key1")).isEmpty();
  }

  @Test
  void size_withEntries_returnsL1Size() {
    hybridCache.put("key1", ByteBuffer.wrap(new byte[] {1}));
    hybridCache.put("key2", ByteBuffer.wrap(new byte[] {2}));

    long size = hybridCache.size();

    assertThat(size).isEqualTo(2);
  }
}
