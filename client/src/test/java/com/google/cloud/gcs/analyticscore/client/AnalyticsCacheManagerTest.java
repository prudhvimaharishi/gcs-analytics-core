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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheManagerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final ByteBuffer FOOTER = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final GcsObjectChunkKey CHUNK_KEY =
      GcsObjectChunkKey.builder().setItemId(ITEM_ID).setGeneration(1L).setChunkIndex(0L).build();

  private AnalyticsCacheManager manager;

  @BeforeEach
  void setUp() {
    // The object-chunk cache is JVM-wide static state; reset it so each test starts clean.
    AnalyticsCacheManager.resetSharedObjectChunkCacheForTesting();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(true).build());
  }

  @AfterEach
  void tearDown() {
    AnalyticsCacheManager.resetSharedObjectChunkCacheForTesting();
  }

  @Test
  void getFooter_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer footer =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return FOOTER.duplicate();
            });
    ByteBuffer secondFooter =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {4, 5, 6});
            });

    assertThat(footer).isEqualTo(FOOTER);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(secondFooter).isEqualTo(FOOTER);
    assertThat(secondFooter).isNotSameInstanceAs(FOOTER);
    assertThat(secondFooter.isReadOnly()).isTrue();
  }

  @Test
  void getFooter_loaderThrowsIOException_rethrowsIOException() {
    assertThrows(
        IOException.class,
        () ->
            manager.getFooter(
                ITEM_ID,
                itemId -> {
                  throw new IOException("test-io-exception");
                }));
  }

  @Test
  void getFooter_cacheDisabled_anyKey_callsLoaderEveryTime() throws IOException {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());
    AtomicInteger callCount = new AtomicInteger(0);
    AnalyticsCacheManager.FooterLoader loader =
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        };

    manager.getFooter(ITEM_ID, loader);
    manager.getFooter(ITEM_ID, loader);

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void invalidateFooter_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateFooter(ITEM_ID);
  }

  @Test
  void invalidateAll_cacheDisabled_anyKey_succeeds() {
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateAll();
  }

  @Test
  void invalidateSmallObject_present_removesEntry() throws IOException {
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    manager = new AnalyticsCacheManager(cacheOptions);
    manager.getSmallObject(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateSmallObject(ITEM_ID);

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getSmallObject(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateFooter_present_removesEntry() throws IOException {
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateFooter(ITEM_ID);

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateAll_withEntries_clearsCache() throws IOException {
    GcsItemId itemId2 = GcsItemId.builder().setBucketName("b").setObjectName("o2").build();
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());
    manager.getFooter(itemId2, itemId -> ByteBuffer.wrap(new byte[] {2}));

    manager.invalidateAll();

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getFooter(
        itemId2,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void objectChunkCache_isSharedAcrossManagers() {
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setObjectChunkCacheEnabled(true)
            .setObjectChunkCacheMaxSizeBytes(1024)
            .build();
    AnalyticsCacheManager writer = new AnalyticsCacheManager(options);
    AnalyticsCacheManager reader = new AnalyticsCacheManager(options);

    writer.putObjectChunk(CHUNK_KEY, ByteBuffer.wrap(new byte[] {7, 8, 9}));

    // A chunk cached via one manager is visible from a separate manager in the same JVM.
    Optional<ByteBuffer> fromReader = reader.getObjectChunkIfPresent(CHUNK_KEY);
    assertThat(fromReader.isPresent()).isTrue();
    assertThat(fromReader.get()).isEqualTo(ByteBuffer.wrap(new byte[] {7, 8, 9}));
    assertThat(fromReader.get().isReadOnly()).isTrue();
  }

  @Test
  void objectChunkCache_firstManagerOptionsWin() {
    AnalyticsCacheManager first =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().setObjectChunkCacheEnabled(true).build());
    first.putObjectChunk(CHUNK_KEY, ByteBuffer.wrap(new byte[] {1}));

    // A later manager that disables the chunk cache still reuses the already-created shared cache.
    AnalyticsCacheManager second =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().setObjectChunkCacheEnabled(false).build());

    assertThat(second.getObjectChunkIfPresent(CHUNK_KEY).isPresent()).isTrue();
  }

  @Test
  void invalidateAll_clearsSharedObjectChunkCache() {
    GcsCacheOptions options = GcsCacheOptions.builder().setObjectChunkCacheEnabled(true).build();
    manager = new AnalyticsCacheManager(options);
    manager.putObjectChunk(CHUNK_KEY, ByteBuffer.wrap(new byte[] {1, 2}));

    manager.invalidateAll();

    assertThat(manager.getObjectChunkIfPresent(CHUNK_KEY).isPresent()).isFalse();
  }
}
