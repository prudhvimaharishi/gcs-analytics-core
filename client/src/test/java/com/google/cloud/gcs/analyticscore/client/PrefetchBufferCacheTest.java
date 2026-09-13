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
import org.junit.jupiter.api.Test;

class PrefetchBufferCacheTest {

  private static final long MAX_SIZE_BYTES = 1024;
  private static final long TTL_SECONDS = 60;
  private static final int BLOCK_SIZE_BYTES = 4;
  private static final long FIRST_BLOCK_OFFSET = 0;
  private static final long SECOND_BLOCK_OFFSET = 4;
  private static final byte[] ONE_BLOCK = {1, 2, 3, 4};
  private static final byte[] OTHER_BLOCK = {5, 6, 7, 8};
  private static final byte[] TWO_BLOCKS = {1, 2, 3, 4, 5, 6, 7, 8};

  @Test
  void constructor_zeroMaxSizeBytes_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PrefetchBufferCache(0, TTL_SECONDS, BLOCK_SIZE_BYTES));

    assertThat(exception).hasMessageThat().isEqualTo("maxSizeBytes must be positive");
  }

  @Test
  void constructor_zeroTtlSeconds_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PrefetchBufferCache(MAX_SIZE_BYTES, 0, BLOCK_SIZE_BYTES));

    assertThat(exception).hasMessageThat().isEqualTo("ttlSeconds must be positive");
  }

  @Test
  void constructor_zeroBlockSizeBytes_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PrefetchBufferCache(MAX_SIZE_BYTES, TTL_SECONDS, 0));

    assertThat(exception).hasMessageThat().isEqualTo("blockSizeBytes must be positive");
  }

  @Test
  void alignDown_positionInsideBlock_returnsBlockStart() {
    PrefetchBufferCache cache = newCache();

    long alignedOffset = cache.alignDown(6);

    assertThat(alignedOffset).isEqualTo(SECOND_BLOCK_OFFSET);
  }

  @Test
  void copyInto_absentBlock_returnsZero() {
    PrefetchBufferCache cache = newCache();
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int copiedBytes = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void copyInto_afterPutRange_copiesStoredBytes() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(ONE_BLOCK);
  }

  @Test
  void copyInto_positionInsideBlock_copiesFromThatPosition() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), 2, destination);

    assertThat(writtenBytes(destination)).isEqualTo(new byte[] {3, 4});
  }

  @Test
  void copyInto_destinationLargerThanBlock_stopsAtBlockBoundary() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(TWO_BLOCKS));
    ByteBuffer destination = ByteBuffer.allocate(TWO_BLOCKS.length);

    int copiedBytes = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(BLOCK_SIZE_BYTES);
  }

  @Test
  void copyInto_calledPerBlock_servesRangeSpanningTwoBlocks() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(TWO_BLOCKS));
    ByteBuffer destination = ByteBuffer.allocate(TWO_BLOCKS.length);

    int firstCopy = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);
    int unusedSecondCopy = cache.copyInto(itemId("object"), firstCopy, destination);

    assertThat(writtenBytes(destination)).isEqualTo(TWO_BLOCKS);
  }

  @Test
  void putRange_multipleBlocks_storesEachBlockSeparately() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(TWO_BLOCKS));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), SECOND_BLOCK_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(OTHER_BLOCK);
  }

  @Test
  void putRange_finalBlockShorterThanBlockSize_storesPartialBlock() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(new byte[] {1, 2, 3, 4, 5}));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int copiedBytes = cache.copyInto(itemId("object"), SECOND_BLOCK_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(1);
  }

  @Test
  void putRange_unalignedRangeStart_throwsIllegalArgumentException() {
    PrefetchBufferCache cache = newCache();

    assertThrows(
        IllegalArgumentException.class,
        () -> cache.putRange(itemId("object"), 2, bufferOf(ONE_BLOCK)));
  }

  @Test
  void putRange_sameOffsetTwice_replacesPreviousBlock() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(OTHER_BLOCK));

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);
    assertThat(writtenBytes(destination)).isEqualTo(OTHER_BLOCK);
  }

  @Test
  void copyInto_differentItemIdsSameOffset_returnsIndependentBlocks() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));
    cache.putRange(itemId("other-object"), FIRST_BLOCK_OFFSET, bufferOf(OTHER_BLOCK));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int unusedCopiedBytes = cache.copyInto(itemId("object"), FIRST_BLOCK_OFFSET, destination);

    assertThat(writtenBytes(destination)).isEqualTo(ONE_BLOCK);
  }

  @Test
  void copyInto_itemIdWithDifferentContentGeneration_returnsZero() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);

    int copiedBytes =
        cache.copyInto(itemIdWithGeneration("object", 2L), FIRST_BLOCK_OFFSET, destination);

    assertThat(copiedBytes).isEqualTo(0);
  }

  @Test
  void isCached_absentBlock_returnsFalse() {
    PrefetchBufferCache cache = newCache();

    boolean cached = cache.isCached(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(cached).isFalse();
  }

  @Test
  void isCached_positionInsideStoredBlock_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));

    boolean cached = cache.isCached(itemId("object"), 3);

    assertThat(cached).isTrue();
  }

  @Test
  void tryClaim_unclaimedBlock_returnsTrue() {
    PrefetchBufferCache cache = newCache();

    boolean claimed = cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(claimed).isTrue();
  }

  @Test
  void tryClaim_alreadyClaimedBlock_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    boolean claimedAgain = cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(claimedAgain).isFalse();
  }

  @Test
  void tryClaim_positionInSameBlock_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    boolean claimedAgain = cache.tryClaim(itemId("object"), 3);

    assertThat(claimedAgain).isFalse();
  }

  @Test
  void tryClaim_differentBlock_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    boolean claimed = cache.tryClaim(itemId("object"), SECOND_BLOCK_OFFSET);

    assertThat(claimed).isTrue();
  }

  @Test
  void tryClaim_afterReleaseClaim_returnsTrue() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);
    cache.releaseClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    boolean claimedAgain = cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(claimedAgain).isTrue();
  }

  @Test
  void isClaimed_unclaimedBlock_returnsFalse() {
    PrefetchBufferCache cache = newCache();

    boolean claimed = cache.isClaimed(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(claimed).isFalse();
  }

  @Test
  void isClaimed_claimedBlock_returnsTrue() {
    PrefetchBufferCache cache = newCache();

    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(cache.isClaimed(itemId("object"), FIRST_BLOCK_OFFSET)).isTrue();
  }

  @Test
  void isClaimed_afterReleaseClaim_returnsFalse() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    cache.releaseClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    assertThat(cache.isClaimed(itemId("object"), FIRST_BLOCK_OFFSET)).isFalse();
  }

  @Test
  void invalidateAll_discardsCachedBlocks() {
    PrefetchBufferCache cache = newCache();
    cache.putRange(itemId("object"), FIRST_BLOCK_OFFSET, bufferOf(ONE_BLOCK));

    cache.invalidateAll();

    assertThat(cache.isCached(itemId("object"), FIRST_BLOCK_OFFSET)).isFalse();
  }

  @Test
  void invalidateAll_discardsClaims() {
    PrefetchBufferCache cache = newCache();
    cache.tryClaim(itemId("object"), FIRST_BLOCK_OFFSET);

    cache.invalidateAll();

    assertThat(cache.isClaimed(itemId("object"), FIRST_BLOCK_OFFSET)).isFalse();
  }

  private static PrefetchBufferCache newCache() {
    return new PrefetchBufferCache(MAX_SIZE_BYTES, TTL_SECONDS, BLOCK_SIZE_BYTES);
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
