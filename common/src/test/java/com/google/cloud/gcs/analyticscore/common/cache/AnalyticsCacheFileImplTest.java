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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AnalyticsCacheFileImplTest {

  @TempDir Path tempDir;

  private AnalyticsCacheFileImpl<String> cache;

  @BeforeEach
  void setUp() {
    cache = AnalyticsCacheFileImpl.create(tempDir, 1024, String::toString);
  }

  @Test
  void get_notPresent_returnsEmpty() {
    String key = "key1";

    Optional<ByteBuffer> result = cache.get(key);

    assertThat(result).isEmpty();
  }

  @Test
  void get_present_returnsValue() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    cache.put(key, buffer);

    Optional<ByteBuffer> result = cache.get(key);

    assertThat(result).hasValue(buffer);
  }

  @Test
  void get_withMappingFunction_notPresent_computesAndCachesValue() throws Exception {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer value =
        cache.get(
            key,
            k -> {
              callCount.incrementAndGet();
              return buffer;
            });
    ByteBuffer secondValue = cache.get(key, k -> ByteBuffer.wrap(new byte[] {4, 5, 6}));

    assertThat(value).isEqualTo(buffer);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(secondValue).isEqualTo(buffer);
  }

  @Test
  void get_withMappingFunction_returnsNull_throwsException() {
    String key = "key1";

    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> cache.get(key, k -> null));

    assertThat(exception).hasMessageThat().contains("mappingFunction returned null");
  }

  @Test
  void get_withMappingFunction_throwsCheckedException_rethrowsException() {
    String key = "key1";

    assertThrows(
        IOException.class,
        () ->
            cache.get(
                key,
                k -> {
                  throw new IOException("test-exception");
                }));
  }

  @Test
  void put_exceedsHighWatermark_evictsOldestEntries() {
    AnalyticsCacheFileImpl<String> smallCache =
        AnalyticsCacheFileImpl.create(tempDir, 100, 0.90, 0.50, 0, String::toString);
    ByteBuffer buffer50 = ByteBuffer.wrap(new byte[50]);
    ByteBuffer buffer45 = ByteBuffer.wrap(new byte[45]);
    ByteBuffer buffer30 = ByteBuffer.wrap(new byte[30]);

    smallCache.put("key1", buffer50);
    smallCache.put("key2", buffer45);
    smallCache.put("key3", buffer30);

    assertThat(smallCache.get("key1")).isEmpty();
    assertThat(smallCache.get("key3")).isPresent();
  }

  @Test
  void invalidate_present_removesEntry() {
    String key = "key1";
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
    cache.put(key, buffer);

    cache.invalidate(key);

    assertThat(cache.get(key)).isEmpty();
  }

  @Test
  void invalidateAll_withEntries_clearsCache() {
    cache.put("key1", ByteBuffer.wrap(new byte[] {1}));
    cache.put("key2", ByteBuffer.wrap(new byte[] {2}));

    cache.invalidateAll();

    assertThat(cache.get("key1")).isEmpty();
    assertThat(cache.get("key2")).isEmpty();
    assertThat(cache.size()).isEqualTo(0);
  }

  @Test
  void size_withEntries_returnsCorrectCount() {
    cache.put("key1", ByteBuffer.wrap(new byte[] {1}));
    cache.put("key2", ByteBuffer.wrap(new byte[] {2}));

    long size = cache.size();

    assertThat(size).isEqualTo(2);
  }
}
