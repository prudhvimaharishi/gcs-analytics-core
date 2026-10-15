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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.FakeVectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.DictionaryTrigger;
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
import org.mockito.Mockito;

class PredictivePrefetchOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int RECORD_COUNT = 500;
  private static final int MULTI_ROW_GROUP_RECORD_COUNT = 5000;
  private static final int FEW_ROW_GROUPS_RECORD_COUNT = 500;
  private static final int SLICE_LENGTH = 64;

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
    optimizer = createOptimizer(/* enabled= */ true);
    cacheManager.getSchemaAccessHistory().get().invalidateAll();
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
    PredictivePrefetchOptimizer disabledOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(false).build());

    assertThat(disabledOptimizer.isApplicable(ITEM_ID)).isFalse();
  }

  @Test
  void onOpen_prefetchDisabledInCacheManager_throwsIllegalStateException() {
    AnalyticsCacheManager disabledCacheManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder().build(),
            GcsPrefetchOptions.builder().setEnabled(false).build(),
            telemetry);
    PredictivePrefetchOptimizer unopenedOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(true).build());

    assertThrows(
        IllegalStateException.class, () -> unopenedOptimizer.onOpen(ITEM_ID, disabledCacheManager));
  }

  @Test
  void onClose_beforeOnOpen_doesNotThrow() {
    PredictivePrefetchOptimizer unopenedOptimizer =
        new PredictivePrefetchOptimizer(GcsPrefetchOptions.builder().setEnabled(true).build());

    unopenedOptimizer.onClose();
  }

  @Test
  void readVectored_withoutFooterLayout_recordsNoCacheMiss() throws IOException {
    cacheManager.invalidateFooter(ITEM_ID);
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(0);
  }

  @Test
  void read_firstReadOfDataPage_returnsZero() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    int servedBytes =
        optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_firstReadOfDataPage_recordsCacheMiss() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_cacheMiss_schedulesNothingBeforeTheForegroundRead() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_firstReadOfDataPage_doesNotScheduleTheChunkBeingRead() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(
        optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
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
    ParquetColumnChunk idChunk = prefetchIdChunk();
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    int servedBytes =
        readThroughChannel(optimizer, prefetchedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_servedFromCache_returnsTheSameBytesAsTheObject() throws IOException {
    ParquetColumnChunk idChunk = prefetchIdChunk();
    long prefetchedOffset = idChunk.getDataPageOffset() + SLICE_LENGTH;

    ByteBuffer destination = ByteBuffer.allocate(SLICE_LENGTH);
    readThroughChannel(optimizer, prefetchedOffset, destination, channel);

    assertThat(destination.array()).isEqualTo(sliceOfContent(prefetchedOffset, SLICE_LENGTH));
  }

  @Test
  void read_servedFromCache_recordsCacheHit() throws IOException {
    ParquetColumnChunk idChunk = prefetchIdChunk();

    readThroughChannel(
        optimizer,
        idChunk.getDataPageOffset() + SLICE_LENGTH,
        ByteBuffer.allocate(SLICE_LENGTH),
        channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_HIT)).isEqualTo(1);
  }

  @Test
  void read_servedFromCache_recordsBytesConsumed() throws IOException {
    ParquetColumnChunk idChunk = prefetchIdChunk();

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
        .get()
        .registerRange(
            ITEM_ID,
            cachedOffset,
            16,
            CompletableFuture.completedFuture(ByteBuffer.wrap(sliceOfContent(cachedOffset, 16))),
            () -> {});

    optimizer.read(cachedOffset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(idChunk.getEndOffset()).isEqualTo(categoryChunk.getStartOffset());
    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void read_withoutFooterLayout_recordsNoCacheMiss() throws IOException {
    cacheManager.invalidateFooter(ITEM_ID);

    optimizer.read(0, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(0);
  }

  @Test
  void readVectored_uncachedRange_recordsCacheMiss() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_MISS)).isEqualTo(1);
  }

  @Test
  void read_offsetBeyondLastRowGroup_returnsZero() throws IOException {
    int servedBytes = optimizer.read(content.length - 16, ByteBuffer.allocate(16), channel);

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
                .get()
                .getDictionaryColumns(layout.getSchemaFingerprint()))
        .contains(ParquetTestFiles.CATEGORY_COLUMN);
  }

  @Test
  void read_emptyDestinationInsideDictionaryPage_recordsNoDictionaryColumn() throws IOException {
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long insideDictionaryPage = categoryChunk.getDictionaryPageOffset().getAsLong() + 1;

    optimizer.read(insideDictionaryPage, ByteBuffer.allocate(0), channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDictionaryColumns(layout.getSchemaFingerprint()))
        .isEmpty();
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
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .containsExactly(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void afterRead_outsideRowGroupsWithOnlyLearnedDataColumn_prefetchesTheSingleRowGroup()
      throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    FakeVectoredSeekableByteChannel newChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer newOptimizer = createOptimizer(/* enabled= */ true);

    newOptimizer.afterRead(content.length - 16, 16, newChannel);
    newOptimizer.onClose();

    assertThat(newChannel.getRequestedOffsets()).containsExactly(idChunk.getStartOffset());
  }

  @Test
  void afterRead_footerOfSingleRowGroupFileWithLearnedDictionaryColumn_prefetchesNoDataPages()
      throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);

    optimizer.afterRead(content.length - 16, 16, channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(idChunk.getStartOffset());
  }

  @Test
  void afterRead_footerCachedUnderGenerationLearnedByChannel_prefetchesTheSingleRowGroup()
      throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    GcsItemId generationItemId =
        GcsItemId.builder()
            .setBucketName(ITEM_ID.getBucketName())
            .setObjectName(ITEM_ID.getObjectName().get())
            .setContentGeneration(7L)
            .build();
    cacheManager.invalidateFooter(ITEM_ID);
    cacheManager.getFooter(generationItemId, id -> ByteBuffer.wrap(content));
    FakeVectoredSeekableByteChannel generationChannel =
        Mockito.spy(new FakeVectoredSeekableByteChannel(content));
    Mockito.doReturn(
            GcsItemInfo.builder().setItemId(generationItemId).setSize(content.length).build())
        .when(generationChannel)
        .getItemInfo();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(layout.getSchemaFingerprint(), ParquetTestFiles.ID_COLUMN);

    optimizer.afterRead(content.length - 16, 16, generationChannel);

    assertThat(generationChannel.getRequestedOffsets()).containsExactly(idChunk.getStartOffset());
  }

  @Test
  void readVectored_nothingPrefetched_returnsAllRanges() throws IOException {
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_returnsNoUnservedRanges() throws IOException {
    ParquetColumnChunk idChunk = prefetchIdChunk();
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeInsidePrefetchedChunk_completesWithObjectBytes() throws Exception {
    ParquetColumnChunk idChunk = prefetchIdChunk();
    long prefetchedOffset = idChunk.getDataPageOffset() + 16;
    GcsObjectRange range = createRange(prefetchedOffset, 16);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(prefetchedOffset, 16));
  }

  @Test
  void readVectored_rangeThatWasNeverPrefetched_returnsTheRange() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(content.length - 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeStillInFlight_returnsNoUnservedRanges() throws IOException {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeStillInFlight_completesOnceThePrefetchArrives() throws Exception {
    long inFlightOffset = scheduleWithoutCompleting();
    GcsObjectRange range = createRange(inFlightOffset, 16);
    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    channel.completeDeferredRanges();

    assertThat(range.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(inFlightOffset, 16));
  }

  @Test
  void readVectored_firstVectoredRequest_recordsTheAccessedColumn() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(
            cacheManager
                .getSchemaAccessHistory()
                .get()
                .getDataColumns(layout.getSchemaFingerprint()))
        .contains(ParquetTestFiles.ID_COLUMN);
  }

  @Test
  void read_dictionaryPageInMultiRowGroupFile_prefetchesLaterDictionariesWithoutLaterDataPages()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    cacheManager
        .getSchemaAccessHistory()
        .get()
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
                .get()
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
                .get()
                .getRangeCovering(
                    ITEM_ID, rg0DataChunk.getStartOffset(), chunkLength(rg0DataChunk)))
        .isEmpty();
  }

  @Test
  void read_dictionaryTriggerBeforeDataColumnsAreKnown_retriesOnNextDictionaryRead()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long rg0DictOffset = rg0DictChunk.getDictionaryPageOffset().getAsLong();
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    readThroughChannel(optimizer, rg0DictOffset, ByteBuffer.allocate(8), channel);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

    readThroughChannel(optimizer, rg0DictOffset + 8, ByteBuffer.allocate(8), channel);

    assertThat(channel.getRequestedOffsets()).contains(rg0DataChunk.getStartOffset());
  }

  @Test
  void read_dictionaryAfterFooterSpeculationQueuedOnlyDictionaries_prefetchesRowGroupDataPages()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk dictChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk dataChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

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
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
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
    for (int index = 0; index < count; index++) {
      ParquetColumnChunk dictChunk =
          columnChunk(fileLayout, index, ParquetTestFiles.CATEGORY_COLUMN);
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
  void read_dictionaryPageAfterDataPageOfSameRowGroup_doesNotPrefetchRowGroupDataPages()
      throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(channel.size() - 16, 16, channel);
    readThroughChannel(
        optimizer, rg0DataChunk.getDataPageOffset(), ByteBuffer.allocate(8), channel);

    readThroughChannel(
        optimizer,
        rg0DictChunk.getDictionaryPageOffset().getAsLong(),
        ByteBuffer.allocate(8),
        channel);

    assertThat(channel.getRequestedOffsets()).doesNotContain(rg0DataChunk.getStartOffset());
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
    optimizer =
        createOptimizer(
            GcsPrefetchOptions.builder().setEnabled(true).setDictionaryTrigger(trigger).build());
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.VALUE_COLUMN);
    return multiRowGroupLayout;
  }

  @Test
  void afterRead_outsideRowGroupsWithLearnedDictionaryColumn_prefetchesDictionaryPagesOnly()
      throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
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

  @Test
  void
      afterRead_singleRowGroupWithFilterAndDataColumn_schedulesOnlyDictionaryThenPrefetchesChunkOnDictionaryRead()
          throws Exception {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long dictOffset = categoryChunk.getDictionaryPageOffset().getAsLong();
    int dictLength = (int) (categoryChunk.getDataPageOffset() - dictOffset);
    channel.deferVectoredCompletion();

    optimizer.afterRead(content.length - 16, 16, channel);

    assertThat(channel.getRequestedRanges()).hasSize(1);
    GcsObjectRange dictPrefetch = channel.getRequestedRanges().get(0);
    assertThat(dictPrefetch.getOffset()).isEqualTo(dictOffset);
    assertThat(dictPrefetch.getLength()).isEqualTo(dictLength);
    dictPrefetch
        .getByteBufferFuture()
        .complete(ByteBuffer.wrap(sliceOfContent(dictOffset, dictLength)));

    // Reading the dictionary triggers the whole-chunk prefetch before serving the dictionary,
    // and serves the dictionary without waiting on the deferred whole-chunk download.
    ByteBuffer dictBuffer = ByteBuffer.allocate(dictLength);
    int servedDictBytes = optimizer.read(dictOffset, dictBuffer, channel);

    assertThat(servedDictBytes).isEqualTo(dictLength);
    assertThat(dictBuffer.array()).isEqualTo(sliceOfContent(dictOffset, dictLength));
    assertThat(channel.getRequestedRanges()).hasSize(2);

    // Now complete the whole-chunk range and verify readVectored over the chunk is served from it.
    channel.completeDeferredRanges();
    GcsObjectRange fullChunkRange = rangeOverChunk(categoryChunk);
    List<GcsObjectRange> unserved =
        optimizer.readVectored(ImmutableList.of(fullChunkRange), ByteBuffer::allocate, channel);

    assertThat(unserved).isEmpty();
    assertThat(fullChunkRange.getByteBufferFuture().get(5, TimeUnit.SECONDS).array())
        .isEqualTo(sliceOfContent(categoryChunk.getStartOffset(), chunkLength(categoryChunk)));
  }

  @Test
  void read_uncachedDictionaryWhenColumnIsAlsoDataColumn_returnsZeroWithoutWaitingOnDataChunk()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long dictOffset = categoryChunk.getDictionaryPageOffset().getAsLong();
    channel.deferVectoredCompletion();

    int servedBytes = optimizer.read(dictOffset, ByteBuffer.allocate(8), channel);

    assertThat(servedBytes).isEqualTo(0);
    assertThat(channel.getRequestedOffsets()).containsExactly(categoryChunk.getStartOffset());
  }

  @Test
  void read_inFlightDictionaryPageFails_attemptsDataPrefetchOnce() throws IOException {
    ParquetFileLayout multiRowGroupLayout = openMultiRowGroupFileWithLearnedFilterSchema();
    ParquetColumnChunk rg0DictChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk rg0DataChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    long dictOffset = rg0DictChunk.getDictionaryPageOffset().getAsLong();
    CompletableFuture<ByteBuffer> dictionaryPrefetch = new CompletableFuture<>();
    cacheManager
        .getPrefetchBufferCache()
        .get()
        .registerRange(
            ITEM_ID,
            dictOffset,
            (int) (rg0DictChunk.getDataPageOffset() - dictOffset),
            dictionaryPrefetch,
            () -> dictionaryPrefetch.completeExceptionally(new IOException("prefetch failed")));
    channel.failVectoredReadsWith(new IOException("prefetch failed"));

    readThroughChannel(optimizer, dictOffset, ByteBuffer.allocate(8), channel);

    assertThat(Collections.frequency(channel.getRequestedOffsets(), rg0DataChunk.getStartOffset()))
        .isEqualTo(1);
  }

  /**
   * Opens a small-row-group file whose schema history already knows {@code id} as a data column and
   * {@code category} as a dictionary column, and returns its layout.
   */
  private ParquetFileLayout openMultiRowGroupFileWithLearnedFilterSchema() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent(FEW_ROW_GROUPS_RECORD_COUNT);
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(/* enabled= */ true);
    int fingerprint = multiRowGroupLayout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    return multiRowGroupLayout;
  }

  /** Leaves a speculative request outstanding and returns the start offset it covers. */
  private long scheduleWithoutCompleting() throws IOException {
    channel.deferVectoredCompletion();
    return prefetchIdChunk().getStartOffset();
  }

  /**
   * Learns {@code id} as a data column and reads the footer so the optimizer prefetches the whole
   * {@code id} chunk of the single row group, then returns that chunk.
   */
  private ParquetColumnChunk prefetchIdChunk() throws IOException {
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(layout.getSchemaFingerprint(), ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(content.length - 16, 16, channel);
    return columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
  }

  /**
   * Reads through {@code reader} the way {@code SmartReadChannel} does: on a cache miss the
   * foreground read happens first and the optimizer then observes it through {@code afterRead}.
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
    return ParquetFooterParser.parse(ByteBuffer.wrap(objectContent)).get();
  }

  private static ParquetColumnChunk columnChunk(
      ParquetFileLayout fileLayout, int rowGroupIndex, String columnPath) {
    return fileLayout.getRowGroup(rowGroupIndex).get().getColumnChunk(columnPath).get();
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
  void afterRead_whenFooterRejectedHistoryDominates_defersPrefetchUntilDictionaryRead()
      throws IOException {
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);

    for (int i = 0; i < 2; i++) {
      FakeVectoredSeekableByteChannel rejectedChannel =
          new FakeVectoredSeekableByteChannel(content);
      PredictivePrefetchOptimizer rejectedOptimizer = createOptimizer(/* enabled= */ true);
      rejectedOptimizer.afterRead(content.length - 16, 16, rejectedChannel);
      rejectedOptimizer.onClose();
      rejectedChannel.close();
    }
    assertThat(cacheManager.getSchemaAccessHistory().get().shouldSpeculateAtFooter(fingerprint))
        .isFalse();

    FakeVectoredSeekableByteChannel thirdChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer thirdOptimizer = createOptimizer(/* enabled= */ true);
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
          .get()
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
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDictionaryAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    for (int i = 0; i < 2; i++) {
      FakeVectoredSeekableByteChannel dictRejectedChannel =
          new FakeVectoredSeekableByteChannel(content);
      PredictivePrefetchOptimizer dictRejectedOptimizer = createOptimizer(/* enabled= */ true);
      readThroughChannel(
          dictRejectedOptimizer,
          categoryChunk.getDictionaryPageOffset().getAsLong(),
          ByteBuffer.allocate(8),
          dictRejectedChannel);
      dictRejectedOptimizer.onClose();
      dictRejectedChannel.close();
    }
    assertThat(cacheManager.getSchemaAccessHistory().get().shouldSpeculateOnDictionary(fingerprint))
        .isFalse();

    FakeVectoredSeekableByteChannel thirdChannel = new FakeVectoredSeekableByteChannel(content);
    PredictivePrefetchOptimizer thirdOptimizer = createOptimizer(/* enabled= */ true);
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
  void afterRead_footerWithAdjacentLearnedColumns_requestsThemAsOneRange() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);

    optimizer.afterRead(content.length - 16, 16, channel);

    assertThat(channel.getRequestedOffsets()).containsExactly(idChunk.getStartOffset());
  }

  @Test
  void readVectored_coalescedRangeAcrossPrefetchedColumns_servesFromCache() throws Exception {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    int fingerprint = layout.getSchemaFingerprint();
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.CATEGORY_COLUMN);
    cacheManager
        .getSchemaAccessHistory()
        .get()
        .recordDataAccess(fingerprint, ParquetTestFiles.ID_COLUMN);
    optimizer.afterRead(content.length - 16, 16, channel);
    long combinedStart = idChunk.getStartOffset();
    int combinedLength = (int) (categoryChunk.getEndOffset() - combinedStart);
    GcsObjectRange coalescedRange =
        GcsObjectRange.builder()
            .setOffset(combinedStart)
            .setLength(combinedLength)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    List<GcsObjectRange> unserved =
        optimizer.readVectored(ImmutableList.of(coalescedRange), ByteBuffer::allocate, channel);

    assertThat(unserved).isEmpty();
    assertThat(coalescedRange.getByteBufferFuture().get().array())
        .isEqualTo(sliceOfContent(combinedStart, combinedLength));
  }

  @Test
  void read_exhaustingPrefetchedColumnChunk_evictsSegmentFromCache() throws IOException {
    ParquetColumnChunk idChunk = prefetchIdChunk();
    long chunkStart = idChunk.getStartOffset();
    int chunkLength = (int) (idChunk.getEndOffset() - chunkStart);

    readThroughChannel(optimizer, chunkStart, ByteBuffer.allocate(chunkLength), channel);

    assertThat(cacheManager.getPrefetchBufferCache().get().getRangeCovering(ITEM_ID, chunkStart, 1))
        .isEmpty();
  }

  private PredictivePrefetchOptimizer createOptimizer(boolean enabled) throws IOException {
    return createOptimizer(GcsPrefetchOptions.builder().setEnabled(enabled).build());
  }

  private PredictivePrefetchOptimizer createOptimizer(GcsPrefetchOptions prefetchOptions)
      throws IOException {
    if (optimizer != null) {
      optimizer.onClose();
    }
    if (cacheManager == null) {
      cacheManager =
          new AnalyticsCacheManager(
              GcsCacheOptions.builder().setFooterCacheEnabled(true).build(),
              prefetchOptions,
              telemetry);
    }
    if (channel != null && channel.size() > 0) {
      ByteBuffer footer = ByteBuffer.wrap(channel.sliceContent(0, (int) channel.size()));
      cacheManager.invalidateFooter(ITEM_ID);
      cacheManager.getFooter(ITEM_ID, id -> footer);
    }
    PredictivePrefetchOptimizer createdOptimizer = new PredictivePrefetchOptimizer(prefetchOptions);
    createdOptimizer.onOpen(ITEM_ID, cacheManager);
    return createdOptimizer;
  }
}
