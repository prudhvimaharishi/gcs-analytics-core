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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheManagerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("b").setObjectName("o").build();
  private static final ByteBuffer FOOTER = ByteBuffer.wrap(new byte[] {1, 2, 3});

  private AnalyticsCacheManager manager;

  @BeforeEach
  void setUp() {
    AnalyticsCacheManager.resetSharedObjectChunkCache();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(true).build());
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
  void getObjectChunk_cacheEnabled_computesAndCachesValue() throws IOException {
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setObjectChunkCacheEnabled(true)
            .setObjectChunkCacheMaxSizeBytes(100)
            .build();
    manager = new AnalyticsCacheManager(options);
    GcsObjectChunkKey key =
        GcsObjectChunkKey.builder().setItemId(ITEM_ID).setGeneration(1L).setChunkIndex(0).build();
    ByteBuffer chunkData = ByteBuffer.wrap(new byte[] {9, 8, 7});
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer result1 =
        manager.getObjectChunk(
            key,
            k -> {
              callCount.incrementAndGet();
              return chunkData.duplicate();
            });
    ByteBuffer result2 =
        manager.getObjectChunk(
            key,
            k -> {
              callCount.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {6, 5, 4});
            });

    assertThat(result1).isEqualTo(chunkData);
    assertThat(result2).isEqualTo(chunkData);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void getObjectChunk_cacheDisabled_callsLoaderEveryTime() throws IOException {
    GcsCacheOptions options = GcsCacheOptions.builder().setObjectChunkCacheEnabled(false).build();
    manager = new AnalyticsCacheManager(options);
    GcsObjectChunkKey key =
        GcsObjectChunkKey.builder().setItemId(ITEM_ID).setGeneration(1L).setChunkIndex(0).build();
    ByteBuffer chunkData = ByteBuffer.wrap(new byte[] {9, 8, 7});
    AtomicInteger callCount = new AtomicInteger(0);

    manager.getObjectChunk(
        key,
        k -> {
          callCount.incrementAndGet();
          return chunkData.duplicate();
        });
    manager.getObjectChunk(
        key,
        k -> {
          callCount.incrementAndGet();
          return chunkData.duplicate();
        });

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void getObjectChunk_sharedAcrossInstances_jvmLevel() throws IOException {
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setObjectChunkCacheEnabled(true)
            .setObjectChunkCacheMaxSizeBytes(100)
            .build();
    AnalyticsCacheManager manager1 = new AnalyticsCacheManager(options);
    AnalyticsCacheManager manager2 = new AnalyticsCacheManager(options);
    GcsObjectChunkKey key =
        GcsObjectChunkKey.builder().setItemId(ITEM_ID).setGeneration(1L).setChunkIndex(0).build();
    ByteBuffer chunkData = ByteBuffer.wrap(new byte[] {9, 8, 7});
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer result1 =
        manager1.getObjectChunk(
            key,
            k -> {
              callCount.incrementAndGet();
              return chunkData.duplicate();
            });
    ByteBuffer result2 =
        manager2.getObjectChunk(
            key,
            k -> {
              callCount.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {6, 5, 4});
            });

    assertThat(result1).isEqualTo(chunkData);
    assertThat(result2).isEqualTo(chunkData);
    assertThat(callCount.get()).isEqualTo(1);
  }
}
