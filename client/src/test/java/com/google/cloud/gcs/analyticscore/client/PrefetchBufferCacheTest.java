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

import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
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
        assertThrows(
            IllegalArgumentException.class,
            () -> new PrefetchBufferCache(0, TTL_SECONDS, new Telemetry(ImmutableList.of())));

    assertThat(exception).hasMessageThat().isEqualTo("maxSizeBytes must be positive");
  }

  @Test
  void constructor_zeroTtlSeconds_throwsIllegalArgumentException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new PrefetchBufferCache(MAX_SIZE_BYTES, 0, new Telemetry(ImmutableList.of())));

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
    PrefetchBufferCache.RegisteredStream stream = cache.registerStream(itemId("object"));
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());
    stream.close();

    boolean registered =
        cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    assertThat(registered).isTrue();
  }

  @Test
  void registerRange_evictedFutureFailsAfterReRegistration_keepsNewRange() {
    PrefetchBufferCache cache = newCache();
    PrefetchBufferCache.RegisteredStream stream = cache.registerStream(itemId("object"));
    CompletableFuture<ByteBuffer> evictedFuture = new CompletableFuture<>();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, evictedFuture);
    stream.close();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    evictedFuture.completeExceptionally(new RuntimeException("cancelled"));

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void getRangeCovering_nestedRanges_returnsSmallestCoveringRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4).get().getLength())
        .isEqualTo(SECOND_RANGE.length);
  }

  @Test
  void getRangeCovering_windowLargerThanNestedRange_returnsEnclosingRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);

    assertThat(cache.getRangeCovering(itemId("object"), 2, 4).get().getLength())
        .isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void registerRange_largerRangeWithSameStart_keepsSmallerRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4).get().getLength())
        .isEqualTo(FIRST_RANGE.length);
  }

  @Test
  void getRangeCovering_inFlightRange_returnsCachedRange() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), 10, 8, new CompletableFuture<>());

    assertThat(cache.getRangeCovering(itemId("object"), 12, 4)).isPresent();
  }

  @Test
  void getRangeCovering_acrossRangesWithGap_returnsEmpty() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), 6, SECOND_RANGE);

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 10)).isEmpty();
  }

  @Test
  void copyIntoAsync_exactRange_returnsStoredBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    ByteBuffer populated =
        cache
            .copyIntoAsync(
                itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length, ByteBuffer::allocate)
            .get()
            .get();

    assertThat(remainingBytes(populated)).isEqualTo(FIRST_RANGE);
  }

  @Test
  void copyIntoAsync_subRange_copiesRequestedBytes() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 10, COMBINED_RANGES);

    ByteBuffer populated =
        cache.copyIntoAsync(itemId("object"), 12, 4, ByteBuffer::allocate).get().get();

    assertThat(remainingBytes(populated)).isEqualTo(new byte[] {3, 4, 5, 6});
  }

  @Test
  void copyIntoAsync_readToRangeEnd_evictsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);

    cache.copyIntoAsync(
        itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length, ByteBuffer::allocate);

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 1)).isEmpty();
  }

  @Test
  void copyIntoAsync_readEndsBeforeRangeEnd_keepsRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 10, COMBINED_RANGES);

    cache.copyIntoAsync(itemId("object"), 12, 4, ByteBuffer::allocate);

    assertThat(cache.getRangeCovering(itemId("object"), 10, 1)).isPresent();
  }

  @Test
  void copyIntoAsync_outOfOrderSubRanges_evictsOnlyAfterAllBytesConsumed() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 10, COMBINED_RANGES);

    cache.copyIntoAsync(itemId("object"), 14, 4, ByteBuffer::allocate);

    assertThat(cache.getRangeCovering(itemId("object"), 10, 4)).isPresent();

    cache.copyIntoAsync(itemId("object"), 10, 4, ByteBuffer::allocate);

    assertThat(cache.getRangeCovering(itemId("object"), 10, 1)).isEmpty();
  }

  @Test
  void copyIntoAsync_acrossAdjacentRanges_stitchesAndEvictsExhaustedSegments() throws Exception {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    ByteBuffer populated =
        cache
            .copyIntoAsync(
                itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate)
            .get()
            .get();

    assertThat(remainingBytes(populated)).isEqualTo(COMBINED_RANGES);
    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 1)).isEmpty();
    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 1)).isEmpty();
  }

  @Test
  void copyIntoAsync_acrossRangesWithGap_returnsEmptyWithoutConsumingFirstSegment() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), 6, SECOND_RANGE);

    assertThat(cache.copyIntoAsync(itemId("object"), FIRST_RANGE_OFFSET, 10, ByteBuffer::allocate))
        .isEmpty();
    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length))
        .isPresent();
  }

  @Test
  void copyIntoAsync_windowMatchesLargerOfNestedRanges_reusesItsBuffer() throws Exception {
    PrefetchBufferCache cache = newCache();
    ByteBuffer combined = bufferOf(COMBINED_RANGES);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);
    cache.registerRange(
        itemId("object"),
        FIRST_RANGE_OFFSET,
        COMBINED_RANGES.length,
        CompletableFuture.completedFuture(combined));

    ByteBuffer populated =
        cache
            .copyIntoAsync(
                itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate)
            .get()
            .get();

    assertThat(populated.array()).isSameInstanceAs(combined.array());
  }

  @Test
  void copyInto_windowMatchesSmallerOfNestedRanges_evictsOnlySmallerRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);

    int unusedCopiedBytes =
        cache.copyInto(
            itemId("object"), SECOND_RANGE_OFFSET, ByteBuffer.allocate(SECOND_RANGE.length));

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4).get().getLength())
        .isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void copyIntoAsync_acrossAdjacentRangesWithNestedRange_skipsNestedRange() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 1, new byte[] {2});
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE);
    registerCompleted(cache, itemId("object"), SECOND_RANGE_OFFSET, SECOND_RANGE);

    var unusedFuture =
        cache.copyIntoAsync(
            itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate);

    assertThat(cache.getRangeCovering(itemId("object"), 1, 1).get().getLength()).isEqualTo(1);
  }

  @Test
  void copyInto_overlappingReads_keepsRangeUntilEveryByteIsRead() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedHeadBytes =
        cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4));

    int unusedOverlappingBytes =
        cache.copyInto(itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(6));

    assertThat(cache.getRangeCovering(itemId("object"), 6, 2)).isPresent();
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
  void copyInto_outOfOrderSubRanges_evictsOnlyAfterAllBytesConsumed() {
    PrefetchBufferCache cache = newCache();
    registerCompleted(cache, itemId("object"), 10, COMBINED_RANGES);

    int unusedTailBytes = cache.copyInto(itemId("object"), 14, ByteBuffer.allocate(4));

    assertThat(cache.getRangeCovering(itemId("object"), 10, 4)).isPresent();

    int unusedHeadBytes = cache.copyInto(itemId("object"), 10, ByteBuffer.allocate(4));

    assertThat(cache.getRangeCovering(itemId("object"), 10, 1)).isEmpty();
  }

  @Test
  void serveFromCache_overlappingReadsOfSameRange_countsConsumedBytesOnce() {
    RecordingOperationListener listener = new RecordingOperationListener();
    PrefetchBufferCache cache =
        new PrefetchBufferCache(
            MAX_SIZE_BYTES, TTL_SECONDS, new Telemetry(ImmutableList.of(listener)));
    registerCompleted(cache, itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES);
    int unusedHeadBytes =
        cache.serveFromCache(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(4), /* recordMiss= */ true);

    int unusedAllBytes =
        cache.serveFromCache(
            itemId("object"),
            FIRST_RANGE_OFFSET,
            ByteBuffer.allocate(COMBINED_RANGES.length),
            /* recordMiss= */ true);

    assertThat(listener.getTotal(Metric.PREFETCH_BYTES_CONSUMED)).isEqualTo(COMBINED_RANGES.length);
  }

  @Test
  void serveVectoredFromCache_inFlightRangeFails_fallsBackToSourceChannel() throws Exception {
    PrefetchBufferCache cache = newCache();
    FakeVectoredSeekableByteChannel channel = new FakeVectoredSeekableByteChannel(COMBINED_RANGES);
    CompletableFuture<ByteBuffer> failedPrefetch = new CompletableFuture<>();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length, failedPrefetch);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(FIRST_RANGE_OFFSET)
            .setLength(FIRST_RANGE.length)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    cache.serveVectoredFromCache(
        itemId("object"),
        ImmutableList.of(range),
        ByteBuffer::allocate,
        channel,
        /* recordMiss= */ true);

    failedPrefetch.completeExceptionally(new IOException("prefetch failed"));

    assertThat(range.getByteBufferFuture().get().array()).isEqualTo(FIRST_RANGE);
  }

  @Test
  void serveVectoredFromCache_inFlightRangeAndFallbackBothFail_completesRangeExceptionally() {
    PrefetchBufferCache cache = newCache();
    FakeVectoredSeekableByteChannel channel = new FakeVectoredSeekableByteChannel(COMBINED_RANGES);
    CompletableFuture<ByteBuffer> failedPrefetch = new CompletableFuture<>();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, FIRST_RANGE.length, failedPrefetch);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(FIRST_RANGE_OFFSET)
            .setLength(FIRST_RANGE.length)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    cache.serveVectoredFromCache(
        itemId("object"),
        ImmutableList.of(range),
        ByteBuffer::allocate,
        channel,
        /* recordMiss= */ true);
    channel.failVectoredReadsWith(new IOException("source read failed"));

    failedPrefetch.completeExceptionally(new IOException("prefetch failed"));

    assertThat(range.getByteBufferFuture().isCompletedExceptionally()).isTrue();
  }

  @Test
  void registerStream_otherItemCached_keepsOtherItemRangesOnClose() {
    PrefetchBufferCache cache = newCache();
    PrefetchBufferCache.RegisteredStream stream = cache.registerStream(itemId("object"));
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());
    cache.registerRange(itemId("other"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    stream.close();

    assertThat(cache.getRangeCovering(itemId("other"), FIRST_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void registerStream_lastStreamClosed_evictsEveryRange() {
    PrefetchBufferCache cache = newCache();
    PrefetchBufferCache.RegisteredStream stream = cache.registerStream(itemId("object"));
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());
    cache.registerRange(itemId("object"), SECOND_RANGE_OFFSET, 4, new CompletableFuture<>());

    stream.close();

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4)).isEmpty();
  }

  @Test
  void registerStream_anotherStreamStillOpen_keepsRangesOnClose() {
    PrefetchBufferCache cache = newCache();
    PrefetchBufferCache.RegisteredStream firstStream = cache.registerStream(itemId("object"));
    PrefetchBufferCache.RegisteredStream unusedSecondStream =
        cache.registerStream(itemId("object"));
    cache.registerRange(itemId("object"), SECOND_RANGE_OFFSET, 4, new CompletableFuture<>());

    firstStream.close();

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void registerStream_closedTwice_doesNotEvictOtherOpenStreamRanges() {
    PrefetchBufferCache cache = newCache();
    PrefetchBufferCache.RegisteredStream firstStream = cache.registerStream(itemId("object"));
    PrefetchBufferCache.RegisteredStream unusedSecondStream =
        cache.registerStream(itemId("object"));
    cache.registerRange(itemId("object"), SECOND_RANGE_OFFSET, 4, new CompletableFuture<>());

    firstStream.close();
    firstStream.close();

    assertThat(cache.getRangeCovering(itemId("object"), SECOND_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void copyInto_registeredWithPromotionCallback_runsCallback() {
    PrefetchBufferCache cache = newCache();
    java.util.concurrent.atomic.AtomicBoolean promoted =
        new java.util.concurrent.atomic.AtomicBoolean();
    cache.registerRange(
        itemId("object"),
        FIRST_RANGE_OFFSET,
        FIRST_RANGE.length,
        CompletableFuture.completedFuture(bufferOf(FIRST_RANGE)),
        () -> promoted.set(true));

    int unusedCopiedBytes =
        cache.copyInto(
            itemId("object"), FIRST_RANGE_OFFSET, ByteBuffer.allocate(FIRST_RANGE.length));

    assertThat(promoted.get()).isTrue();
  }

  @Test
  void copyIntoAsync_acrossAdjacentRanges_promotesEverySegment() {
    PrefetchBufferCache cache = newCache();
    java.util.concurrent.atomic.AtomicInteger promotedCount =
        new java.util.concurrent.atomic.AtomicInteger();
    cache.registerRange(
        itemId("object"),
        FIRST_RANGE_OFFSET,
        FIRST_RANGE.length,
        CompletableFuture.completedFuture(bufferOf(FIRST_RANGE)),
        promotedCount::incrementAndGet);
    cache.registerRange(
        itemId("object"),
        SECOND_RANGE_OFFSET,
        SECOND_RANGE.length,
        CompletableFuture.completedFuture(bufferOf(SECOND_RANGE)),
        promotedCount::incrementAndGet);

    var unusedFuture =
        cache.copyIntoAsync(
            itemId("object"), FIRST_RANGE_OFFSET, COMBINED_RANGES.length, ByteBuffer::allocate);

    assertThat(promotedCount.get()).isEqualTo(2);
  }

  @Test
  void evictRange_registeredFuture_evictsRange() {
    PrefetchBufferCache cache = newCache();
    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, future);

    cache.evictRange(itemId("object"), FIRST_RANGE_OFFSET, 4, future);

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
  }

  @Test
  void evictRange_differentFuture_keepsRange() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    cache.evictRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isPresent();
  }

  @Test
  void invalidateAll_discardsInFlightRanges() {
    PrefetchBufferCache cache = newCache();
    cache.registerRange(itemId("object"), FIRST_RANGE_OFFSET, 4, new CompletableFuture<>());

    cache.invalidateAll();

    assertThat(cache.getRangeCovering(itemId("object"), FIRST_RANGE_OFFSET, 4)).isEmpty();
  }

  private static PrefetchBufferCache newCache() {
    return new PrefetchBufferCache(MAX_SIZE_BYTES, TTL_SECONDS, new Telemetry(ImmutableList.of()));
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
