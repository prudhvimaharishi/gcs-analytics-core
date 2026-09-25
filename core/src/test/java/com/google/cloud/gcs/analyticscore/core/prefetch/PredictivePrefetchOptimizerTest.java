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

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeVectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.DictionaryTrigger;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.PrefetchMode;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PredictivePrefetchOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int RECORD_COUNT = 500;
  private static final int MULTI_ROW_GROUP_RECORD_COUNT = 5000;
  private static final int FEW_ROW_GROUPS_RECORD_COUNT = 500;
  private static final int SLICE_LENGTH = 64;
  private static final int BLOCK_SIZE_BYTES = 8 * 1024 * 1024;
  private static final int WHOLE_FILE_BLOCK_SIZE_BYTES = 1024 * 1024;

  @TempDir File temporaryDirectory;

  private byte[] content;
  private ParquetFileLayout layout;
  private FakeVectoredSeekableByteChannel channel;
  private RecordingOperationListener metricListener;
  private Telemetry telemetry;
  private AnalyticsCacheManager cacheManager;
  private PredictivePrefetchOptimizer optimizer;

  @BeforeEach
  void createOptimizerOverParquetObject() throws IOException {
    content =
        ParquetTestFiles.readAllBytes(
            ParquetTestFiles.createFile(
                temporaryDirectory,
                "data.parquet",
                RECORD_COUNT,
                ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES));
    layout = parseLayout(content);
    metricListener = new RecordingOperationListener();
    telemetry = new Telemetry(ImmutableList.of(metricListener));
    channel = new FakeVectoredSeekableByteChannel(content);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    cacheManager.getSchemaAccessHistory().invalidateAll();
  }

  @AfterEach
  void closeOptimizer() {
    optimizer.onClose();
    telemetry.close();
    channel.close();
  }

  @Test
  void isApplicable_parquetObject_returnsTrue() {
    assertThat(optimizer.isApplicable(ITEM_ID)).isTrue();
  }

  @Test
  void isApplicable_uppercaseParquetExtension_returnsTrue() {
    GcsItemId uppercaseItemId =
        GcsItemId.builder().setBucketName("bucket").setObjectName("DATA.PARQUET").build();

    assertThat(optimizer.isApplicable(uppercaseItemId)).isTrue();
  }

  @Test
  void isApplicable_nonParquetObject_returnsFalse() {
    GcsItemId csvItemId =
        GcsItemId.builder().setBucketName("bucket").setObjectName("data.csv").build();

    assertThat(optimizer.isApplicable(csvItemId)).isFalse();
  }

  @Test
  void isApplicable_prefetchDisabled_returnsFalse() {
    PredictivePrefetchOptimizer disabledOptimizer = createOptimizer(PrefetchMode.DISABLED);

    assertThat(disabledOptimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void read_firstReadOfDataPage_returnsZero() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    int servedBytes =
        readThroughChannel(
            optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_firstReadOfDataPage_recordsCacheMiss() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_cacheMiss_schedulesNothingBeforeTheForegroundRead() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_firstReadOfDataPage_schedulesTheUnreadRemainderOfTheColumnChunk() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).contains(idChunk.getDataPageOffset() + SLICE_LENGTH);
  }

  @Test
  void read_firstReadOfDataPage_doesNotScheduleBytesAlreadyRead() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(idChunk.getDataPageOffset());
  }

  @Test
  void read_afterSpeculation_servesTheRequestedLength() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    int servedBytes =
        readThroughChannel(optimizer, prefetchedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_servedFromCache_returnsTheSameBytesAsTheObject() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    ByteBuffer destination = ByteBuffer.allocate(SLICE_LENGTH);
    readThroughChannel(optimizer, prefetchedOffset, destination, channel);

    assertThat(destination.array()).isEqualTo(sliceOfContent(prefetchedOffset, SLICE_LENGTH));
  }

  @Test
  void read_servedFromCache_recordsCacheHit() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    readThroughChannel(
        optimizer,
        idChunk.getDataPageOffset() + SLICE_LENGTH,
        ByteBuffer.allocate(SLICE_LENGTH),
        channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_HIT)).isEqualTo(1);
  }

  @Test
  void read_servedFromCache_recordsBytesConsumed() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    readThroughChannel(
        optimizer,
        idChunk.getDataPageOffset() + SLICE_LENGTH,
        ByteBuffer.allocate(SLICE_LENGTH),
        channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_BYTES_CONSUMED)).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_partialCacheHit_recordsOnlyTheServedBytesAsAccessed() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long cachedOffset = categoryChunk.getStartOffset() - 16;
    cacheManager
        .getPrefetchBufferCache()
        .registerRange(
            ITEM_ID,
            cachedOffset,
            16,
            CompletableFuture.completedFuture(ByteBuffer.wrap(sliceOfContent(cachedOffset, 16))));

    readThroughChannel(optimizer, cachedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(idChunk.getEndOffset()).isEqualTo(categoryChunk.getStartOffset());
    assertThat(cacheManager.getSchemaAccessHistory().getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void read_withoutFooterLayout_recordsNoCacheMiss() throws IOException {
    GcsPrefetchOptions prefetchOptions =
        GcsPrefetchOptions.builder().setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP).build();
    AnalyticsCacheManager disabledFooterCacheManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().setFooterCacheEnabled(false).build(), prefetchOptions);
    PredictivePrefetchOptimizer optimizerWithoutLayout =
        new PredictivePrefetchOptimizer(prefetchOptions, telemetry);
    optimizerWithoutLayout.onOpen(ITEM_ID, disabledFooterCacheManager);

    readThroughChannel(optimizerWithoutLayout, 0, ByteBuffer.allocate(SLICE_LENGTH), channel);
    optimizerWithoutLayout.onClose();

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(0);
  }

  @Test
  void readVectoredWithChannel_uncachedRange_recordsCacheMiss() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_offsetBeyondLastRowGroup_returnsZero() throws IOException {
    int servedBytes =
        readThroughChannel(optimizer, content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_offsetBeyondLastRowGroup_schedulesNothingWhenSchemaIsUnlearned() throws IOException {
    readThroughChannel(optimizer, content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_footerNotInGlobalCache_skipsPrefetching() throws IOException {
    cacheManager.invalidateFooter(ITEM_ID);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_dictionaryPageOffset_recordsTheColumnAsDictionaryOnly() throws IOException {
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long dictionaryOffset = categoryChunk.getDictionaryPageOffset().getAsLong();

    readThroughChannel(optimizer, dictionaryOffset, ByteBuffer.allocate(16), channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .getDictionaryColumns(layout.getSchemaFingerprint()))
        .contains(ParquetTestFiles.CATEGORY_COLUMN);
  }

  @Test
  void read_alreadyCachedRemainder_schedulesNoAdditionalDuplicateRequests() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);
    int scheduledAfterFirstRead = channel.getRequestedOffsets().size();

    readThroughChannel(
        optimizer,
        idChunk.getDataPageOffset() + SLICE_LENGTH,
        ByteBuffer.allocate(SLICE_LENGTH),
        channel);

    assertThat(channel.getRequestedOffsets()).hasSize(scheduledAfterFirstRead);
  }

  @Test
  void read_smallSliceOfOneColumn_doesNotRecordOtherColumns() throws IOException {
    optimizer.onClose();
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP, WHOLE_FILE_BLOCK_SIZE_BYTES);
    cacheManager.getSchemaAccessHistory().invalidateAll();
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);

    assertThat(cacheManager.getSchemaAccessHistory().getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void read_rowGroupMode_schedulesTheExactColumnRangeInTheNextRowGroup() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk firstIdChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk secondIdChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, firstIdChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets()).contains(secondIdChunk.getStartOffset());
  }

  @Test
  void afterRead_outsideRowGroupsWithLearnedSchema_prefetchesTheFirstRowGroup() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    FakeVectoredSeekableByteChannel newChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer newOptimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);

    newOptimizer.afterRead(content.length - 16, 16, newChannel);
    newOptimizer.onClose();

    assertThat(newChannel.getRequestedOffsets()).contains(idChunk.getStartOffset());
  }

  @Test
  void readVectored_nothingPrefetched_returnsAllRanges() {
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_returnsNoUnservedRanges() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_completesWithObjectBytes() throws Exception {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    long prefetchedOffset = idChunk.getDataPageOffset() + 16;
    GcsObjectRange range = createRange(prefetchedOffset, 16);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(prefetchedOffset, 16));
  }

  @Test
  void readVectored_rangeThatWasNeverPrefetched_returnsTheRange() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(content.length - 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeStillInFlight_returnsNoUnservedRanges() throws IOException {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeStillInFlight_completesOnceThePrefetchArrives() throws Exception {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);
    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    channel.completeDeferredRanges();

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(inFlightOffset, 16));
  }

  @Test
  void readVectoredWithChannel_firstVectoredRequest_recordsTheAccessedColumn() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(cacheManager.getSchemaAccessHistory().getDataColumns(layout.getSchemaFingerprint()))
        .contains(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void readVectoredWithChannel_beforeTheAfterHook_schedulesNothing() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk firstIdChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(firstIdChunk)), ByteBuffer::allocate, channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void afterReadVectored_firstVectoredRequest_schedulesTheNextRowGroupExactRange()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk firstIdChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk secondIdChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    List<GcsObjectRange> ranges = ImmutableList.of(rangeOverChunk(firstIdChunk));
    List<GcsObjectRange> unserved = optimizer.readVectored(ranges, ByteBuffer::allocate, channel);
    channel.readVectored(unserved, ByteBuffer::allocate);

    optimizer.afterReadVectored(ranges, channel);

    assertThat(channel.getRequestedOffsets()).contains(secondIdChunk.getStartOffset());
  }

  @Test
  void afterReadVectored_rowGroupsRequestedInReverseOrder_doesNotRejectTheEarlierRowGroup()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0IdChunk = columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1IdChunk = columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg2IdChunk = columnChunk(multiRowGroupLayout, 2, ParquetTestFiles.ID_COLUMN);
    List<GcsObjectRange> ranges =
        ImmutableList.of(rangeOverChunk(rg1IdChunk), rangeOverChunk(rg0IdChunk));
    List<GcsObjectRange> unserved = optimizer.readVectored(ranges, ByteBuffer::allocate, channel);
    channel.readVectored(unserved, ByteBuffer::allocate);

    optimizer.afterReadVectored(ranges, channel);

    assertThat(channel.getRequestedOffsets()).contains(rg2IdChunk.getStartOffset());
  }

  @Test
  void afterReadVectored_noFollowingRowGroup_schedulesNothing() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    List<GcsObjectRange> ranges = ImmutableList.of(rangeOverChunk(idChunk));
    optimizer.readVectored(ranges, ByteBuffer::allocate, channel);

    optimizer.afterReadVectored(ranges, channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void afterReadVectored_rangeOutsideAnyRowGroup_schedulesNothing() throws IOException {
    List<GcsObjectRange> ranges = ImmutableList.of(createRange(content.length - 16, 16));
    optimizer.readVectored(ranges, ByteBuffer::allocate, channel);

    optimizer.afterReadVectored(ranges, channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void readVectoredWithChannel_rangeInsidePrefetchedChunk_returnsNoUnservedRanges()
      throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void afterReadVectored_skipsRowGroupOmittedDuringUpfrontDictionarySweep() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg2DictChunk =
        columnChunk(multiRowGroupLayout, 2, ParquetTestFiles.CATEGORY_COLUMN);
    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);
    readThroughChannel(
        optimizer,
        rg2DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg2DataChunk =
        columnChunk(multiRowGroupLayout, 2, ParquetTestFiles.ID_COLUMN);
    List<GcsObjectRange> ranges = ImmutableList.of(rangeOverChunk(rg0DataChunk));
    List<GcsObjectRange> unserved = optimizer.readVectored(ranges, ByteBuffer::allocate, channel);
    channel.readVectored(unserved, ByteBuffer::allocate);

    optimizer.afterReadVectored(ranges, channel);

    assertThat(channel.getRequestedOffsets()).contains(rg2DataChunk.getStartOffset());
    assertThat(channel.getRequestedOffsets()).doesNotContain(rg1DataChunk.getStartOffset());
  }

  @Test
  void readVectored_coalescedRangeAcrossAdjacentPrefetchedColumns_servesFromCache()
      throws Exception {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk rg0FirstChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg0SecondChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    List<GcsObjectRange> rg0Ranges =
        ImmutableList.of(rangeOverChunk(rg0FirstChunk), rangeOverChunk(rg0SecondChunk));
    List<GcsObjectRange> unservedRg0 =
        optimizer.readVectored(rg0Ranges, ByteBuffer::allocate, channel);
    channel.readVectored(unservedRg0, ByteBuffer::allocate);
    optimizer.afterReadVectored(rg0Ranges, channel);

    ParquetColumnChunk rg1FirstChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1SecondChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    long coalescedStart = Math.min(rg1FirstChunk.getStartOffset(), rg1SecondChunk.getStartOffset());
    long coalescedEnd = Math.max(rg1FirstChunk.getEndOffset(), rg1SecondChunk.getEndOffset());
    int coalescedLength = (int) (coalescedEnd - coalescedStart);
    GcsObjectRange coalescedRange = createRange(coalescedStart, coalescedLength);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(coalescedRange), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
    assertThat(coalescedRange.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(
            Arrays.copyOfRange(
                multiRowGroupContent,
                (int) coalescedStart,
                (int) coalescedStart + coalescedLength));
  }

  @Test
  void read_dictionaryPageInMultiRowGroupFile_prefetchesLaterDictionariesWithoutLaterDataPages()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    cacheManager
        .getSchemaAccessHistory()
        .recordDataAccess(multiRowGroupLayout.getSchemaFingerprint(), ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets())
        .contains(rg1DictChunk.getDictionaryPageOffset().getAsLong());
    assertThat(channel.getRequestedOffsets()).doesNotContain(rg1DataChunk.getStartOffset());
  }

  @Test
  void read_allKnownDictionariesReadInSmallRowGroup_prefetchesThatRowGroupsDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_dictionariesOfLaterRowGroupWhileEarlierPrefetchPending_doesNotPrefetchItsDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    readThroughChannel(
        optimizer,
        rg1DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(rg1DataChunk.getStartOffset());
  }

  @Test
  void read_dictionarySweepAcrossRowGroups_keepsFirstRowGroupDataPrefetch() throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);

    readDictionariesOfFirstRowGroups(multiRowGroupLayout, 3);

    assertThat(
            cacheManager
                .getPrefetchBufferCache()
                .getRangeCovering(
                    ITEM_ID, rg0DataChunk.getStartOffset(), chunkLength(rg0DataChunk)))
        .isPresent();
  }

  @Test
  void read_dataPageOfLaterRowGroupAfterSweep_evictsSkippedRowGroupDataPrefetch()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    readDictionariesOfFirstRowGroups(multiRowGroupLayout, 3);

    readThroughChannel(
        optimizer, rg1DataChunk.getDataPageOffset(), ByteBuffer.allocate(8), channel);

    assertThat(
            cacheManager
                .getPrefetchBufferCache()
                .getRangeCovering(
                    ITEM_ID, rg0DataChunk.getStartOffset(), chunkLength(rg0DataChunk)))
        .isEmpty();
  }

  @Test
  void read_dataPageOfFirstRowGroupAfterSweep_prefetchesNextSweptRowGroupDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1DataChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    readDictionariesOfFirstRowGroups(multiRowGroupLayout, 3);

    readThroughChannel(
        optimizer, rg0DataChunk.getDataPageOffset(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg1DataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryTriggerBeforeDataColumnsAreKnown_retriesOnNextDictionaryRead()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long rg0DictOffset = rg0DictChunk.getDictionaryPageOffset().getAsLong();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, rg0DictOffset, ByteBuffer.allocate(8), channel);
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(optimizer, rg0DictOffset + 8, ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryAfterFooterSpeculationQueuedOnlyDictionaries_prefetchesRowGroupDataPages()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk dictChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk dataChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer,
        dictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).contains(dataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryTriggerWithUncachedDictionaryPage_prefetchesTheWholeColumnChunk()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0Chunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);

    readThroughChannel(
        optimizer, rg0Chunk.getDictionaryPageOffset().getAsLong(), ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0Chunk.getStartOffset());
  }

  /** Reads the {@code category} dictionary page of each of the first {@code count} row groups. */
  private void readDictionariesOfFirstRowGroups(ParquetFileLayout fileLayout, int count)
      throws IOException {
    for (int ordinal = 0; ordinal < count; ordinal++) {
      ParquetColumnChunk dictChunk =
          columnChunk(fileLayout, ordinal, ParquetTestFiles.CATEGORY_COLUMN);
      readThroughChannel(
          optimizer,
          dictChunk.getDictionaryPageOffset().getAsLong(),
          ByteBuffer.allocate(8),
          channel);
    }
  }

  private static int chunkLength(ParquetColumnChunk chunk) {
    return (int) (chunk.getEndOffset() - chunk.getStartOffset());
  }

  @Test
  void read_dictionaryPageAfterFooterPrefetch_doesNotRefetchConsumedLaterDictionaries()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    long rg1DictOffset = rg1DictChunk.getDictionaryPageOffset().getAsLong();
    optimizer.afterRead(channel.size() - 16, 16, channel);
    GcsObjectRange rg1DictRange =
        createRange(rg1DictOffset, (int) (rg1DictChunk.getDataPageOffset() - rg1DictOffset));
    optimizer.readVectored(ImmutableList.of(rg1DictRange), ByteBuffer::allocate, channel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(Collections.frequency(channel.getRequestedOffsets(), rg1DictOffset)).isEqualTo(1);
  }

  @Test
  void read_dictionaryPageAfterFailedFooterDictionaryPrefetch_retriesLaterDictionaries()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    FakeVectoredSeekableByteChannel failingChannel =
        new FakeVectoredSeekableByteChannel(channel.sliceContent(0, (int) channel.size()));
    failingChannel.failVectoredReadsWith(new IOException("prefetch rejected"));
    optimizer.afterRead(channel.size() - 16, 16, failingChannel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets())
        .contains(rg1DictChunk.getDictionaryPageOffset().getAsLong());
  }

  @Test
  void read_dictionaryPageAfterAsyncDictionaryDownloadFailure_retriesLaterDictionaries()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long rg1DictOffset =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN)
            .getDictionaryPageOffset()
            .getAsLong();
    channel.deferVectoredCompletion();
    optimizer.afterRead(channel.size() - 16, 16, channel);
    for (GcsObjectRange requestedRange : channel.getRequestedRanges()) {
      requestedRange
          .getByteBufferFuture()
          .completeExceptionally(new IOException("download failed"));
    }

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(Collections.frequency(channel.getRequestedOffsets(), rg1DictOffset)).isEqualTo(2);
  }

  /**
   * Opens a small-row-group file, with few enough row groups that all dictionary pages fit in one
   * prefetch batch, whose schema history already knows {@code id} as a data column and {@code
   * category} as a dictionary column, and returns its layout.
   */
  private ParquetFileLayout openMultiRowGroupFileWithLearnedFilterSchema() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    return multiRowGroupLayout;
  }

  @Test
  void read_firstOfTwoKnownDictionariesWithFirstDictReadTrigger_prefetchesRowGroupDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout =
        openFileWithTwoLearnedDictionaryColumns(DictionaryTrigger.FIRST_DICT_READ);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_firstOfTwoKnownDictionariesWithLastDictReadTrigger_doesNotPrefetchRowGroupDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout =
        openFileWithTwoLearnedDictionaryColumns(DictionaryTrigger.LAST_DICT_READ);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(rg0DataChunk.getStartOffset());
  }

  /**
   * Opens a small-row-group file whose schema history knows {@code id} as a data column and both
   * {@code category} and {@code value} as dictionary columns, and returns its layout.
   */
  private ParquetFileLayout openFileWithTwoLearnedDictionaryColumns(DictionaryTrigger trigger)
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP, BLOCK_SIZE_BYTES, trigger);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.VALUE_COLUMN);
    return multiRowGroupLayout;
  }

  @Test
  void afterRead_outsideRowGroupsWithLearnedDictionaryColumn_prefetchesDictionaryPagesOnly()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg1DictChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.afterRead(multiRowGroupContent.length - 16, 16, channel);

    assertThat(channel.getRequestedOffsets())
        .containsAtLeast(
            rg0DictChunk.getDictionaryPageOffset().getAsLong(),
            rg1DictChunk.getDictionaryPageOffset().getAsLong());
    assertThat(channel.getRequestedOffsets()).doesNotContain(rg0DataChunk.getStartOffset());
  }

  /** Leaves a speculative request outstanding and returns the start offset it covers. */
  private long scheduleWithoutCompleting() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    channel.deferVectoredCompletion();
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    return channel.getRequestedOffsets().get(0);
  }

  /**
   * Reads through {@code readingOptimizer} the way {@code SmartReadChannel} does: on a cache miss
   * the foreground read happens first and the optimizer then observes it through {@code afterRead}.
   */
  private static int readThroughChannel(
      PredictivePrefetchOptimizer reader,
      long position,
      ByteBuffer destination,
      FakeVectoredSeekableByteChannel source)
      throws IOException {
    int requestedLength = destination.remaining();
    int servedBytes = reader.read(position, destination, source);
    if (servedBytes == 0) {
      reader.afterRead(position, requestedLength, source);
    }
    return servedBytes;
  }

  private static ParquetFileLayout parseLayout(byte[] objectContent) {
    return ParquetFooterParser.parse(ByteBuffer.wrap(objectContent), objectContent.length).get();
  }

  private static ParquetColumnChunk columnChunk(
      ParquetFileLayout fileLayout, int rowGroupOrdinal, String columnPath) {
    return fileLayout.getRowGroup(rowGroupOrdinal).get().getColumnChunk(columnPath).get();
  }

  private static GcsObjectRange createRange(long offset, int length) {
    return GcsObjectRange.builder()
        .setOffset(offset)
        .setLength(length)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private static GcsObjectRange rangeOverChunk(ParquetColumnChunk chunk) {
    return createRange(
        chunk.getStartOffset(), (int) (chunk.getEndOffset() - chunk.getStartOffset()));
  }

  private byte[] createMultiRowGroupContent() throws IOException {
    return createMultiRowGroupContent(MULTI_ROW_GROUP_RECORD_COUNT);
  }

  private byte[] createMultiRowGroupContent(int recordCount) throws IOException {
    File multiRowGroupFile =
        ParquetTestFiles.createFile(
            temporaryDirectory,
            "multi.parquet",
            recordCount,
            ParquetTestFiles.TINY_ROW_GROUP_SIZE_BYTES);
    return ParquetTestFiles.readAllBytes(multiRowGroupFile);
  }

  private byte[] sliceOfContent(long offset, int length) {
    return Arrays.copyOfRange(content, (int) offset, (int) offset + length);
  }

  @Test
  void read_footerCacheDisabled_skipsPrefetching() throws IOException {
    GcsPrefetchOptions prefetchOptions =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP)
            .setBlockSizeBytes(BLOCK_SIZE_BYTES)
            .build();
    AnalyticsCacheManager disabledFooterCacheManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().setFooterCacheEnabled(false).build(), prefetchOptions);
    PredictivePrefetchOptimizer optimizerWithDisabledFooterCache =
        new PredictivePrefetchOptimizer(prefetchOptions, telemetry);
    optimizerWithDisabledFooterCache.onOpen(ITEM_ID, disabledFooterCacheManager);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizerWithDisabledFooterCache,
        idChunk.getDataPageOffset(),
        ByteBuffer.allocate(16),
        channel);
    optimizerWithDisabledFooterCache.onClose();

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void afterReadVectored_withInFlightForegroundRange_defersSpeculationUntilComplete()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk rg0Id = columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk rg1Id = columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);
    GcsObjectRange rg0Range = rangeOverChunk(rg0Id);

    List<GcsObjectRange> unserved =
        optimizer.readVectored(ImmutableList.of(rg0Range), ByteBuffer::allocate, channel);
    optimizer.afterReadVectored(ImmutableList.of(rg0Range), channel);

    assertThat(unserved).containsExactly(rg0Range);
    assertThat(channel.getRequestedOffsets()).doesNotContain(rg1Id.getStartOffset());

    rg0Range.getByteBufferFuture().complete(ByteBuffer.allocate(rg0Range.getLength()));

    assertThat(channel.getRequestedOffsets()).contains(rg1Id.getStartOffset());
  }

  @Test
  void afterRead_whenFooterRejectedHistoryDominates_defersPrefetchUntilDictionaryRead()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);

    // Simulate two files closed immediately after footer read (FOOTER_REJECTED).
    for (int i = 0; i < 2; i++) {
      FakeVectoredSeekableByteChannel rejectedChannel =
          new FakeVectoredSeekableByteChannel(content);
      PredictivePrefetchOptimizer rejectedOptimizer =
          createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
      rejectedOptimizer.afterRead(content.length - 16, 16, rejectedChannel);
      rejectedOptimizer.onClose();
      rejectedChannel.close();
    }
    assertThat(cacheManager.getSchemaAccessHistory().shouldSpeculateAtFooter(fingerprint))
        .isFalse();

    // Third file: footer read should NOT trigger prefetch, but subsequent dictionary read SHOULD.
    FakeVectoredSeekableByteChannel thirdChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer thirdOptimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    thirdOptimizer.afterRead(content.length - 16, 16, thirdChannel);
    assertThat(thirdChannel.getRequestedOffsets()).isEmpty();

    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(
        thirdOptimizer,
        categoryChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        thirdChannel);
    thirdOptimizer.onClose();
    thirdChannel.close();

    assertThat(thirdChannel.getRequestedOffsets()).contains(idChunk.getStartOffset());
  }

  @Test
  void read_dictionaryPageWithFooterSpeculationDeferred_doesNotRefetchTheDictionaryJustRead()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    for (int i = 0; i < 2; i++) {
      cacheManager
          .getSchemaAccessHistory()
          .recordFileOutcome(fingerprint, SchemaAccessHistory.FileFilterOutcome.FOOTER_REJECTED);
    }
    long rg0DictOffset =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN)
            .getDictionaryPageOffset()
            .getAsLong();
    long rg1DictOffset =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.CATEGORY_COLUMN)
            .getDictionaryPageOffset()
            .getAsLong();
    optimizer.afterRead(channel.size() - 16, 16, channel);

    readThroughChannel(optimizer, rg0DictOffset, ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg1DictOffset);
    assertThat(channel.getRequestedOffsets()).doesNotContain(rg0DictOffset);
  }

  @Test
  void read_whenDictRejectedHistoryDominates_doesNotSpeculateDataOnDictionaryRead()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager.getSchemaAccessHistory().recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    // Simulate two files that read dictionary pages and then closed without reading data pages.
    for (int i = 0; i < 2; i++) {
      FakeVectoredSeekableByteChannel dictRejectedChannel =
          new FakeVectoredSeekableByteChannel(content);
      PredictivePrefetchOptimizer dictRejectedOptimizer =
          createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
      readThroughChannel(
          dictRejectedOptimizer,
          categoryChunk.getDictionaryPageOffset().getAsLong(),
          ByteBuffer.allocate(8),
          dictRejectedChannel);
      dictRejectedOptimizer.onClose();
      dictRejectedChannel.close();
    }
    assertThat(cacheManager.getSchemaAccessHistory().shouldSpeculateOnDictionary(fingerprint))
        .isFalse();

    FakeVectoredSeekableByteChannel thirdChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer thirdOptimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    thirdOptimizer.afterRead(content.length - 16, 16, thirdChannel);
    readThroughChannel(
        thirdOptimizer,
        categoryChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        thirdChannel);
    thirdOptimizer.onClose();
    thirdChannel.close();

    assertThat(thirdChannel.getRequestedOffsets()).doesNotContain(idChunk.getStartOffset());
  }

  @Test
  void read_columnExceedingConfiguredBlockSizeBytes_splitsPrefetchIntoConfiguredSlices()
      throws IOException {
    int customBlockSizeBytes = 128;
    optimizer.onClose();
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP, customBlockSizeBytes);
    cacheManager.getSchemaAccessHistory().invalidateAll();
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    long firstSliceStart = idChunk.getDataPageOffset() + SLICE_LENGTH;
    long secondSliceStart = idChunk.getDataPageOffset() + customBlockSizeBytes;

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).containsAtLeast(firstSliceStart, secondSliceStart);
  }

  private PredictivePrefetchOptimizer createOptimizer(PrefetchMode prefetchMode) {
    return createOptimizer(prefetchMode, BLOCK_SIZE_BYTES);
  }

  private PredictivePrefetchOptimizer createOptimizer(
      PrefetchMode prefetchMode, int blockSizeBytes) {
    return createOptimizer(prefetchMode, blockSizeBytes, DictionaryTrigger.LAST_DICT_READ);
  }

  private PredictivePrefetchOptimizer createOptimizer(
      PrefetchMode prefetchMode, int blockSizeBytes, DictionaryTrigger dictionaryTrigger) {
    GcsPrefetchOptions prefetchOptions =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(prefetchMode)
            .setBlockSizeBytes(blockSizeBytes)
            .setDictionaryTrigger(dictionaryTrigger)
            .build();
    if (cacheManager == null) {
      cacheManager =
          new AnalyticsCacheManager(
              GcsCacheOptions.builder().setFooterCacheEnabled(true).build(), prefetchOptions);
    }
    if (channel != null && channel.size() > 0) {
      cacheManager.putFooter(
          ITEM_ID, ByteBuffer.wrap(channel.sliceContent(0, (int) channel.size())));
    }
    PredictivePrefetchOptimizer createdOptimizer =
        new PredictivePrefetchOptimizer(prefetchOptions, telemetry);
    createdOptimizer.onOpen(ITEM_ID, cacheManager);
    return createdOptimizer;
  }
}
