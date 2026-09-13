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
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.PrefetchMode;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.RecordingOperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PredictivePrefetchOptimizerTest {

  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName("bucket").setObjectName("data.parquet").build();
  private static final int RECORD_COUNT = 500;
  private static final int MULTI_ROW_GROUP_RECORD_COUNT = 5000;
  private static final int SLICE_LENGTH = 64;
  private static final int BLOCK_SIZE_BYTES = 64;

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
  void read_firstReadOfDataPage_schedulesTheBlockHoldingThatColumn() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).contains(blockStart(idChunk.getDataPageOffset()));
  }

  @Test
  void read_firstReadOfDataPage_schedulesBlockAlignedOffsetsOnly() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(unalignedRequestedOffsets()).isEmpty();
  }

  @Test
  void read_afterSpeculation_servesTheRequestedLength() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    int servedBytes =
        optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_spanningTwoBlocks_servesBytesFromBothBlocks() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    long unalignedOffset = blockStart(idChunk.getDataPageOffset()) + BLOCK_SIZE_BYTES - 8;
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    ByteBuffer destination = ByteBuffer.allocate(16);
    optimizer.read(unalignedOffset, destination, channel);

    assertThat(destination.array()).isEqualTo(sliceOfContent(unalignedOffset, 16));
  }

  @Test
  void read_servedFromCache_returnsTheSameBytesAsTheObject() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    long offset = idChunk.getDataPageOffset();
    optimizer.read(offset, ByteBuffer.allocate(SLICE_LENGTH), channel);

    ByteBuffer destination = ByteBuffer.allocate(SLICE_LENGTH);
    optimizer.read(offset, destination, channel);

    assertThat(destination.array()).isEqualTo(sliceOfContent(offset, SLICE_LENGTH));
  }

  @Test
  void read_servedFromCache_recordsCacheHit() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_CACHE_HIT)).isEqualTo(1);
  }

  @Test
  void read_servedFromCache_recordsBytesConsumed() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(metricListener.getTotal(Metric.PREFETCH_BYTES_CONSUMED)).isEqualTo(SLICE_LENGTH);
  }

  @Test
  void read_offsetBeyondLastRowGroup_returnsZero() throws IOException {
    int servedBytes = optimizer.read(content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_offsetBeyondLastRowGroup_schedulesNothing() throws IOException {
    optimizer.read(content.length - 16, ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_objectIsNotParquet_returnsZero() throws IOException {
    channel = new FakeVectoredSeekableByteChannel(new byte[4096]);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);

    int servedBytes = optimizer.read(0, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(servedBytes).isEqualTo(0);
  }

  @Test
  void read_objectIsNotParquet_schedulesNothing() throws IOException {
    channel = new FakeVectoredSeekableByteChannel(new byte[4096]);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);

    optimizer.read(0, ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void read_dictionaryPageOffset_schedulesTheBlockHoldingTheDictionary() throws IOException {
    ParquetColumnChunk categoryChunk = columnChunk(layout, 0, ParquetTestFiles.CATEGORY_COLUMN);
    long dictionaryOffset = categoryChunk.getDictionaryPageOffset().getAsLong();

    optimizer.read(dictionaryOffset, ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets()).contains(blockStart(dictionaryOffset));
  }

  @Test
  void read_sameBlockReadTwice_schedulesNoAdditionalBlocks() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);
    int scheduledAfterFirstRead = channel.getRequestedOffsets().size();

    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(SLICE_LENGTH), channel);

    assertThat(channel.getRequestedOffsets()).hasSize(scheduledAfterFirstRead);
  }

  @Test
  void read_rowGroupMode_schedulesTheAccessedColumnInTheNextRowGroup() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk firstIdChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk secondIdChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);

    optimizer.read(firstIdChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);

    assertThat(channel.getRequestedOffsets())
        .contains(blockStart(secondIdChunk.getDataPageOffset()));
  }

  @Test
  void readVectored_nothingPrefetched_returnsAllRanges() {
    GcsObjectRange range = createRange(0, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).containsExactly(range);
  }

  @Test
  void readVectored_rangeInsidePrefetchedBlock_returnsNoUnservedRanges() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 8, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).isEmpty();
  }

  @Test
  void readVectored_rangeInsidePrefetchedBlock_completesWithObjectBytes() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 8, SLICE_LENGTH);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(range.getByteBufferFuture().join().array())
        .isEqualTo(sliceOfContent(idChunk.getDataPageOffset() + 8, SLICE_LENGTH));
  }

  @Test
  void readVectored_rangeThatWasNeverPrefetched_returnsTheRange() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(content.length - 16, 16);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate);

    assertThat(unservedRanges).containsExactly(range);
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
  void readVectoredWithChannel_firstVectoredRequest_schedulesTheNextRowGroup() throws IOException {
    byte[] multiRowGroupContent = createMultiRowGroupContent();
    ParquetFileLayout multiRowGroupLayout = parseLayout(multiRowGroupContent);
    channel = new FakeVectoredSeekableByteChannel(multiRowGroupContent);
    optimizer = createOptimizer(PrefetchMode.PREDICTIVE_ROW_GROUP);
    ParquetColumnChunk firstIdChunk =
        columnChunk(multiRowGroupLayout, 0, ParquetTestFiles.ID_COLUMN);
    ParquetColumnChunk secondIdChunk =
        columnChunk(multiRowGroupLayout, 1, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(firstIdChunk)), ByteBuffer::allocate, channel);

    assertThat(channel.getRequestedOffsets()).contains(blockStart(secondIdChunk.getStartOffset()));
  }

  @Test
  void readVectoredWithChannel_noFollowingRowGroup_schedulesNothing() throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);

    optimizer.readVectored(
        ImmutableList.of(rangeOverChunk(idChunk)), ByteBuffer::allocate, channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void readVectoredWithChannel_rangeOutsideAnyRowGroup_schedulesNothing() throws IOException {
    GcsObjectRange range = createRange(content.length - 16, 16);

    optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(channel.getRequestedOffsets()).isEmpty();
  }

  @Test
  void readVectoredWithChannel_rangeInsidePrefetchedBlock_returnsNoUnservedRanges()
      throws IOException {
    ParquetColumnChunk idChunk = columnChunk(layout, 0, ParquetTestFiles.ID_COLUMN);
    optimizer.read(idChunk.getDataPageOffset(), ByteBuffer.allocate(16), channel);
    GcsObjectRange range = createRange(idChunk.getDataPageOffset() + 8, SLICE_LENGTH);

    List<GcsObjectRange> unservedRanges =
        optimizer.readVectored(ImmutableList.of(range), ByteBuffer::allocate, channel);

    assertThat(unservedRanges).isEmpty();
  }

  private static ParquetFileLayout parseLayout(byte[] objectContent) {
    return ParquetFooterParser.parse(ByteBuffer.wrap(objectContent), objectContent.length).get();
  }

  private static ParquetColumnChunk columnChunk(
      ParquetFileLayout fileLayout, int rowGroupOrdinal, String columnPath) {
    return fileLayout.getRowGroup(rowGroupOrdinal).get().getColumnChunk(columnPath).get();
  }

  private static long blockStart(long offset) {
    return offset - (offset % BLOCK_SIZE_BYTES);
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

  private List<Long> unalignedRequestedOffsets() {
    return channel.getRequestedOffsets().stream()
        .filter(offset -> offset % BLOCK_SIZE_BYTES != 0)
        .collect(ImmutableList.toImmutableList());
  }

  private byte[] createMultiRowGroupContent() throws IOException {
    File multiRowGroupFile =
        ParquetTestFiles.createFile(
            temporaryDirectory,
            "multi.parquet",
            MULTI_ROW_GROUP_RECORD_COUNT,
            ParquetTestFiles.TINY_ROW_GROUP_SIZE_BYTES);
    return ParquetTestFiles.readAllBytes(multiRowGroupFile);
  }

  private byte[] sliceOfContent(long offset, int length) {
    return Arrays.copyOfRange(content, (int) offset, (int) offset + length);
  }

  private PredictivePrefetchOptimizer createOptimizer(PrefetchMode prefetchMode) {
    GcsPrefetchOptions prefetchOptions =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(prefetchMode)
            .setBlockSizeBytes(BLOCK_SIZE_BYTES)
            .build();
    cacheManager = new AnalyticsCacheManager(GcsCacheOptions.builder().build(), prefetchOptions);
    PredictivePrefetchOptimizer createdOptimizer =
        new PredictivePrefetchOptimizer(prefetchOptions, telemetry);
    createdOptimizer.onOpen(ITEM_ID, cacheManager);
    return createdOptimizer;
  }
}
