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
  void copyInto_afterPutRange_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_positionInsideRange_copiesFromThatPosition() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), 2, destination);

    assertThat(writtenBytes(destination)).isEqualTo(new byte[] {3, 4});
  }

  @Test
  void copyInto_unalignedRangeStart_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), 13, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int copiedBytes = cache.copyInto(itemId("object"), 13, destination);

    assertThat(copiedBytes).isEqualTo(FIRST_RANGE.length);
    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_calledAcrossAdjacentRanges_servesCombinedBytes() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("object"), SECOND_RANGE_OFFSET, bufferOf(SECOND_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(COMBINED_RANGES.length);

    int firstCopy = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);
    int unusedSecondCopy = cache.copyInto(itemId("object"), firstCopy, destination);

    assertThat(writtenBytes(destination)).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void putRange_sameOffsetTwice_replacesPreviousRange() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(SECOND_RANGE));

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);
    assertThat(writtenBytes(destination)).isEqualTo(SECOND_RANGE);
  }

  @Test
  void copyInto_differentItemIdsSameOffset_returnsIndependentRanges() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("other-object"), FIRST_RANGE_OFFSET, bufferOf(SECOND_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyInto_itemIdWithDifferentContentGeneration_returnsZero() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int copiedBytes =
        cache.copyInto(itemIdWithGeneration("object", 2L), FIRST_RANGE_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void isCached_absentRange_returnsFalse() {
    PrefetchBufferCache cache = newCache();

    boolean cached = cache.isCached(itemId("object"), FIRST_RANGE_OFFSET);

    assertThat(cached).isFalse();
  }

  @Test
  void isCached_positionInsideStoredRange_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));

    boolean cached = cache.isCached(itemId("object"), 3);

    assertThat(cached).isTrue();
  }

  @Test
  void isRangeCached_fullyCoveredRange_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), 10, bufferOf(COMBINED_RANGES));

    assertThat(cache.isRangeCached(itemId("object"), 12, 4)).isTrue();
  }

  @Test
  void isRangeCached_partiallyMissingRange_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), 10, bufferOf(FIRST_RANGE));

    assertThat(cache.isRangeCached(itemId("object"), 10, 6)).isFalse();
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
    assertThat(
            cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>()))
        .isTrue();
  }

  @Test
  void getRangeCovering_inFlightRange_returnsCachedRange() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    cache.registerRange(itemId("object"), 10, 8, future);

    assertThat(cache.getRangeCovering(itemId("object"), 12, 4)).isPresent();
    assertThat(cache.isRangeCached(itemId("object"), 12, 4)).isFalse();

    future.complete(bufferOf(COMBINED_RANGES));

    assertThat(cache.isRangeCached(itemId("object"), 12, 4)).isTrue();
  }

  @Test
  void invalidateAll_discardsCachedRanges() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));

    cache.invalidateAll();

    assertThat(cache.isCached(itemId("object"), FIRST_RANGE_OFFSET)).isFalse();
  }

  @Test
  void invalidateAll_discardsInFlightRanges() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    cache.invalidateAll();

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
  }

  @Test
  void getRangeCovering_acrossAdjacentRanges_returnsStitchedRange() throws Exception {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("object"), SECOND_RANGE_OFFSET, bufferOf(SECOND_RANGE));

    CachedRange stitched =
        cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length).get();
    ByteBuffer slice = stitched.slice(FIRST_RANGE_OFFSET, COMBINED_RANGES.length).get();
    byte[] actual = new byte[slice.remaining()];
    slice.get(actual);

    assertThat(actual).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void getRangeCovering_acrossRangesWithGap_returnsEmpty() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("object"), 6, bufferOf(SECOND_RANGE));

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 10)).isEmpty();
  }

  @Test
  void isRangeCached_acrossAdjacentCompletedRanges_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("object"), SECOND_RANGE_OFFSET, bufferOf(SECOND_RANGE));

    assertThat(cache.isRangeCached(itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length))
        .isTrue();
  }

  @Test
  void consumeRange_acrossAdjacentRanges_populatesTarget() throws Exception {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    cache.putRange(itemId("object"), SECOND_RANGE_OFFSET, bufferOf(SECOND_RANGE));

    ByteBuffer populated =
        cache
            .consumeRange(
                itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate)
            .get()
            .get();
    byte[] actual = new byte[populated.remaining()];
    populated.get(actual);

    assertThat(actual).isEqualTo(COMBINED_RANGES);
  }

  @Test
  void copyInto_whenSegmentExhausted_preservesSegmentUntilConsumedOrEvicted() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_RANGE_OFFSET, bufferOf(FIRST_RANGE));
    ByteBuffer destination = ByteBuffer.allocate(FIRST_RANGE.length);

    int copied = cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, destination);

    assertThat(copied).isEqualTo(FIRST_RANGE.length);
    assertThat(cache.isCached(itemId("object"), FIRST_RANGE_OFFSET)).isTrue();
    cache.evictRange(itemId("object"), FIRST_RANGE_OFFSET);
    assertThat(cache.isCached(itemId("object"), FIRST_RANGE_OFFSET)).isFalse();
  }

  private static PrefetchBufferCache newCache() {
    return new PrefetchBufferCache(MAX_SIZE_BYTES, TTL_SECONDS);
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
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }
}
