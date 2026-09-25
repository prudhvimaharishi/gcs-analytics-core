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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class PrefetchBufferCacheTest {

  private static final long MAX_SIZE_BYTES = 1024;
  private static final long TTL_SECONDS = 60;
  private static final long FIRST_RANGE_OFFSET = 0;
  private static final long SECOND_RANGE_OFFSET = 4;
  private static final byte[] FIRST_RANGE = {1, 2, 3, 4};
  private static final byte[] SECOND_RANGE = {5, 6, 7, 8};
  private static final byte[] COMBINED_RANGES = {1, 2, 3, 4, 5, 6, 7, 8};

  @Test
  void constructor_zeroMaxSizeBytes_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new PrefetchBufferCache(0, TTL_SECONDS));

    assertThat(exception).hasMessageThat().isEqualTo("maxSizeBytes must be positive");
  }

  @Test
  void constructor_zeroTtlSeconds_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> new PrefetchBufferCache(MAX_SIZE_BYTES, 0));

    assertThat(exception).hasMessageThat().isEqualTo("ttlSeconds must be positive");
  }

  @Test
  void copyInto_absentRange_returnsZero() {
    PrefetchBufferCache cache = newCache();
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int copiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_completedRange_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_positionInsideRange_copiesFromThatPosition() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), 2, destination);

    assertThat(writtenBytes(destination)).isEqualTo(new byte[] {3, 4});
  }

  @Test
  void copyInto_unalignedRangeStart_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 13, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), 13, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_acrossAdjacentRanges_servesCombinedBytes() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(COMBINED_RANGES.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void copyInto_differentItemIdsSameOffset_returnsIndependentRanges() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("other-object"), FIRST_RANGE_OFFSET, SECOND_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_itemIdWithDifferentContentGeneration_returnsZero() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int copiedBytes =
        cache.copyInto(itemIdWithGeneration("object", 2L), FIRST_RANGE_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_segmentExhausted_evictsSegment() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int unusedCopiedBytes =
        cache.copyInto(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(FIRST_RANGE.length));

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 1)).isEmpty();
  }

  @Test
  void copyInto_segmentPartiallyRead_keepsSegment() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    int unusedCopiedBytes =
        cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(2));

    assertThat(cache.getRangeCovering(itemId("object"), 2, 2)).isPresent();
  }

  @Test
  void registerRange_unregisteredRange_returnsTrue() {
    PrefetchBufferCache cache = newCache();

    boolean registered =
        cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_alreadyCoveredByInFlightRange_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), 10, 8, new CompletableFuture<>());

    boolean registeredAgain =
        cache.registerRange(itemId("object"), 12, 4, new CompletableFuture<>());

    assertThat(registeredAgain).isFalse();
  }

  @Test
  void registerRange_differentRange_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    boolean registered =
        cache.registerRange(itemId("object"), SECOND_RANGE_OFFSET, 4, new CompletableFuture<>());

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_failedFuture_isRemovedAutomatically() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, future);

    future.completeExceptionally(new RuntimeException("network error"));

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
  }

  @Test
  void registerRange_afterAllRangesOfItemEvicted_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());
    cache.evictRange(itemId("object"), FIRST_RANGE_OFFSET);

    boolean registered =
        cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    assertThat(registered).isTrue();
  }

  @Test
  void getRangeCovering_inFlightRange_returnsCachedRange() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), 10, 8, new CompletableFuture<>());

    assertThat(cache.getRangeCovering(itemId("object"), 12, 4)).isPresent();
  }

  @Test
  void getRangeCovering_acrossAdjacentRanges_returnsEmpty() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length))
        .isEmpty();
  }

  @Test
  void consumeRange_exactRange_returnsStoredBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    ByteBuffer populated =
        cache
            .consumeRange(
                itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length, ByteBuffer::allocate)
            .get()
            .get();

    assertThat(remainingBytes(populated)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void consumeRange_subRange_copiesRequestedBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 10, COMBINED_RANGES);

    ByteBuffer populated =
        cache.consumeRange(itemId("object"), 12, 4, ByteBuffer::allocate).get().get();

    assertThat(remainingBytes(populated)).isEqualTo(new byte[] {3, 4, 5, 6});
  }

  @Test
  void consumeRange_acrossAdjacentRanges_returnsEmpty() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    assertThat(
            cache.consumeRange(
                itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate))
        .isEmpty();
  }

  @Test
  void evictRangeWindow_overlappingRanges_evictsEveryOverlappingRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    cache.evictRangeWindow(itemId("object"), 2, 6);

    assertThat(cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(8)))
        .isEqualTo(0);
  }

  @Test
  void evictRangeWindow_rangeOutsideWindow_keepsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    cache.evictRangeWindow(itemId("object"), FIRST_RANGE_OFFSET, SECOND_RANGE_OFFSET);

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void evictAllForItem_evictsOnlyThatItem() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("other-object"), FIRST_RANGE_OFFSET, SECOND_RANGE);

    cache.evictAllForItem(itemId("object"));

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
    assertThat(cache.getRangeCovering(itemId("other-object"), FIRST_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void invalidateAll_discardsInFlightRanges() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    cache.invalidateAll();

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
  }

  private static PrefetchBufferCache newCache() {
    return new PrefetchBufferCache(MAX_SIZE_BYTES, TTL_SECONDS);
  }

  private static void registerCompleted(
      PrefetchBufferCache cache, GcsItemId itemId, long offset, byte[] bytes) {
    cache.registerRange(
        itemId, offset, bytes.length, CompletableFuture.completedFuture(bufferOf(bytes)));
  }

  private static GcsItemId itemId(String objectName) {
    return GcsItemId.builder().setBucketName("test-bucket").setObjectName(objectName).build();
  }

  private static GcsItemId itemIdWithGeneration(String objectName, long contentGeneration) {
    return GcsItemId.builder()
        .setBucketName("test-bucket")
        .setObjectName(objectName)
        .setContentGeneration(contentGeneration)
        .build();
  }

  private static ByteBuffer bufferOf(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  private static byte[] writtenBytes(ByteBuffer buffer) {
    ByteBuffer view = buffer.duplicate();
    view.flip();
    return remainingBytes(view);
  }

  private static byte[] remainingBytes(ByteBuffer buffer) {
    ByteBuffer view = buffer.duplicate();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }
}
