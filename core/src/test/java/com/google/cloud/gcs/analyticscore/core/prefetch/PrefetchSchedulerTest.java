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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.FakeVectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PrefetchSchedulerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int CONTENT_LENGTH = 512;
  private static final int BLOCK_SIZE_BYTES = 32;
  private static final int MAX_CONCURRENT_BLOCKS = 4;
  private static final long BLOCK_OFFSET = 64;

  private FakeVectoredSeekableByteChannel channel;
  private PrefetchBufferCache bufferCache;
  private RecordingOperationListener metricListener;
  private Telemetry telemetry;
  private PrefetchScheduler scheduler;

  @BeforeEach
  void createScheduler() {
    channel = new FakeVectoredSeekableByteChannel(createContent());
    bufferCache = new PrefetchBufferCache(CONTENT_LENGTH, 60, BLOCK_SIZE_BYTES);
    metricListener = new RecordingOperationListener();
    telemetry = new Telemetry(ImmutableList.of(metricListener));
    scheduler = new PrefetchScheduler(bufferCache, telemetry, MAX_CONCURRENT_BLOCKS);
  }

  @AfterEach
  void closeScheduler() {
    scheduler.close();
    telemetry.close();
    channel.close();
  }

  @Test
  void schedule_uncachedBlock_requestsTheBlockRange() {
    schedule(BLOCK_OFFSET);

    assertThat(channel.getRequestedOffsets()).containsExactly(BLOCK_OFFSET);
  }

  @Test
  void schedule_uncachedBlock_publishesBlockContentIntoCache() {
    schedule(BLOCK_OFFSET);

    assertThat(readCachedBlock(BLOCK_OFFSET))
        .isEqualTo(channel.sliceContent((int) BLOCK_OFFSET, BLOCK_SIZE_BYTES));
  }

  @Test
  void schedule_uncachedBlock_recordsBytesLoaded() {
    schedule(BLOCK_OFFSET);

    assertThat(metricListener.getTotal(Metric.PREFETCH_BYTES_LOADED)).isEqualTo(BLOCK_SIZE_BYTES);
  }

  @Test
  void schedule_completedBlock_releasesTheClaim() {
    schedule(BLOCK_OFFSET);

    assertThat(bufferCache.isClaimed(ITEM_ID, BLOCK_OFFSET)).isFalse();
  }

  @Test
  void schedule_consecutiveBlocks_requestsEachBlockAsItsOwnRange() {
    schedule(0, BLOCK_SIZE_BYTES);

    assertThat(channel.getRequestedOffsets())
        .containsExactly(0L, (long) BLOCK_SIZE_BYTES)
        .inOrder();
  }

  @Test
  void schedule_blockAlreadyCached_doesNotRequestTheRange() {
    bufferCache.putRange(
        ITEM_ID,
        BLOCK_OFFSET,
        ByteBuffer.wrap(channel.sliceContent((int) BLOCK_OFFSET, BLOCK_SIZE_BYTES)));

    schedule(BLOCK_OFFSET);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_blockAlreadyClaimed_doesNotRequestTheRange() {
    bufferCache.tryClaim(ITEM_ID, BLOCK_OFFSET);

    schedule(BLOCK_OFFSET);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_sameBlockTwice_requestsTheRangeOnlyOnce() {
    channel.deferVectoredCompletion();

    schedule(BLOCK_OFFSET);
    schedule(BLOCK_OFFSET);

    assertThat(channel.getRequestedOffsets()).containsExactly(BLOCK_OFFSET);
  }

  @Test
  void schedule_blockStartingBeyondFileSize_doesNotRequestTheRange() {
    scheduler.schedule(channel, ITEM_ID, ImmutableList.of((long) CONTENT_LENGTH), CONTENT_LENGTH);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_finalBlockShorterThanBlockSize_requestsOnlyTheRemainingBytes() {
    scheduler.schedule(channel, ITEM_ID, ImmutableList.of(BLOCK_OFFSET), BLOCK_OFFSET + 8);

    assertThat(readCachedBlock(BLOCK_OFFSET))
        .isEqualTo(channel.sliceContent((int) BLOCK_OFFSET, 8));
  }

  @Test
  void schedule_emptyBlockList_doesNotRequestAnyRange() {
    scheduler.schedule(channel, ITEM_ID, ImmutableList.of(), CONTENT_LENGTH);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void schedule_moreBlocksThanConcurrencyLimit_requestsOnlyUpToTheLimit() {
    schedule(0, 32, 64, 96, 128, 160);

    assertThat(channel.getRequestedOffsets()).containsExactly(0L, 32L, 64L, 96L).inOrder();
  }

  @Test
  void schedule_blockBeyondConcurrencyLimit_isNotClaimed() {
    schedule(0, 32, 64, 96, 128);

    assertThat(bufferCache.isClaimed(ITEM_ID, 128)).isFalse();
  }

  @Test
  void schedule_channelFails_releasesTheClaim() {
    channel.failVectoredReadsWith(new IOException("vectored read rejected"));

    schedule(BLOCK_OFFSET);

    assertThat(bufferCache.isClaimed(ITEM_ID, BLOCK_OFFSET)).isFalse();
  }

  @Test
  void schedule_channelFails_leavesTheBlockUncached() {
    channel.failVectoredReadsWith(new IOException("vectored read rejected"));

    schedule(BLOCK_OFFSET);

    assertThat(bufferCache.isCached(ITEM_ID, BLOCK_OFFSET)).isFalse();
  }

  @Test
  void schedule_channelFails_clearsInFlightTracking() {
    channel.failVectoredReadsWith(new IOException("vectored read rejected"));

    schedule(BLOCK_OFFSET);

    assertThat(scheduler.getInFlightOffsets()).isEmpty();
  }

  @Test
  void schedule_requestNotSettled_tracksTheBlockAsInFlight() {
    channel.deferVectoredCompletion();

    schedule(BLOCK_OFFSET);

    assertThat(scheduler.getInFlightOffsets()).containsExactly(BLOCK_OFFSET);
  }

  @Test
  void cancelAll_withPendingRequest_clearsInFlightTracking() {
    channel.deferVectoredCompletion();
    schedule(BLOCK_OFFSET);

    scheduler.cancelAll();

    assertThat(scheduler.getInFlightOffsets()).isEmpty();
  }

  @Test
  void close_withPendingRequest_clearsInFlightTracking() {
    channel.deferVectoredCompletion();
    schedule(BLOCK_OFFSET);

    scheduler.close();

    assertThat(scheduler.getInFlightOffsets()).isEmpty();
  }

  @Test
  void cancelAll_withPendingRequest_leavesTheBlockUncached() {
    channel.deferVectoredCompletion();
    schedule(BLOCK_OFFSET);

    scheduler.cancelAll();

    assertThat(bufferCache.isCached(ITEM_ID, BLOCK_OFFSET)).isFalse();
  }

  private static byte[] createContent() {
    byte[] content = new byte[CONTENT_LENGTH];
    for (int index = 0; index < CONTENT_LENGTH; index++) {
      content[index] = (byte) (index % 251);
    }
    return content;
  }

  private void schedule(long... blockOffsets) {
    ImmutableList.Builder<Long> offsets = ImmutableList.builder();
    for (long blockOffset : blockOffsets) {
      offsets.add(blockOffset);
    }
    scheduler.schedule(channel, ITEM_ID, offsets.build(), CONTENT_LENGTH);
  }

  private byte[] readCachedBlock(long blockOffset) {
    ByteBuffer destination = ByteBuffer.allocate(BLOCK_SIZE_BYTES);
    int copiedBytes = bufferCache.copyInto(ITEM_ID, blockOffset, destination);
    byte[] bytes = new byte[copiedBytes];
    destination.flip();
    destination.get(bytes);
    return bytes;
  }
}
