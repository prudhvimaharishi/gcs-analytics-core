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

import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.columnChunk;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.dictionaryEncodedColumnChunk;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.layout;
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.rowGroup;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.DictionaryTrigger;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory.FileFilterOutcome;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowGroupPrefetchTrackerTest {

  private static final int SCHEMA_FINGERPRINT = 7;
  private static final long MAX_BLOCK_SIZE_BYTES = 1024;
  private static final long SPLIT_SIZE_BYTES = 64L * 1024 * 1024;

  private ParquetFileLayout fileLayout;
  private SchemaAccessHistory accessHistory;
  private List<Range<Long>> cancelledWindows;
  private RowGroupPrefetchTracker tracker;

  @BeforeEach
  void createTracker() {
    fileLayout =
        layout(
            rowGroup(
                0,
                columnChunk("id", 100, 50),
                dictionaryEncodedColumnChunk("category", 150, 50, 170),
                columnChunk("value", 250, 50)),
            rowGroup(
                1,
                columnChunk("id", 400, 50),
                dictionaryEncodedColumnChunk("category", 450, 50, 470)));
    accessHistory = new SchemaAccessHistory();
    cancelledWindows = new ArrayList<>();
    tracker =
        new RowGroupPrefetchTracker(
            fileLayout,
            accessHistory,
            MAX_BLOCK_SIZE_BYTES,
            DictionaryTrigger.LAST_DICT_READ,
            (start, end) -> cancelledWindows.add(Range.closedOpen(start, end)));
  }

  @Test
  void onSingleRead_dataPageRead_recordsColumnInHistory() {
    tracker.onSingleRead(100, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("id");
  }

  @Test
  void onSingleRead_secondSliceInsideSameDataPage_skipsReobserving() {
    tracker.onSingleRead(100, 16, ranges -> true);
    accessHistory.invalidateAll();

    tracker.onSingleRead(116, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void onSingleRead_dictionaryPageReadWithLearnedColumns_schedulesLaterDictAndMergedDataRanges() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "category");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(150, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges)
        .containsExactly(Range.closedOpen(450L, 470L), Range.closedOpen(100L, 200L))
        .inOrder();
  }

  @Test
  void onSingleRead_dictionaryPageReadTwiceAfterSuccess_schedulesNothingOnSecondRead() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    tracker.onSingleRead(150, 8, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(158, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onSingleRead_dictionaryPageReadWhenScheduleFails_retriesOnNextDictionaryRead() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    tracker.onSingleRead(150, 8, ranges -> false);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(158, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges)
        .containsExactly(Range.closedOpen(450L, 470L), Range.closedOpen(100L, 150L))
        .inOrder();
  }

  @Test
  void onSingleRead_chunkReadFromDictionaryPage_doesNotRecordDictionaryColumn() {
    tracker.onSingleRead(150, 50, ranges -> true);

    assertThat(accessHistory.getDictionaryColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void onSingleRead_dictionaryPageReadBeforeChunkRead_keepsDictionaryColumn() {
    tracker.onSingleRead(150, 20, ranges -> true);

    tracker.onSingleRead(150, 50, ranges -> true);

    assertThat(accessHistory.getDictionaryColumns(SCHEMA_FINGERPRINT)).containsExactly("category");
  }

  @Test
  void onSingleRead_dictionaryPageReadAfterDataPageInSameRowGroup_doesNotScheduleDataColumns() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    tracker.onSingleRead(900, 16, ranges -> true);
    tracker.onSingleRead(100, 16, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(150, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void
      onSingleRead_dictionaryPageInLaterRowGroupWhileEarlierPrefetchPending_skipsLaterDataPrefetch() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    tracker.onSingleRead(900, 16, ranges -> true);
    tracker.onSingleRead(150, 8, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(450, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void
      onSingleRead_dictionaryPageInLaterRowGroupWhenFirstRowGroupSkipped_schedulesLaterDataColumns() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    tracker.onSingleRead(900, 16, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(450, 8, scheduledRanges::addAll);

    assertThat(scheduledRanges).containsExactly(Range.closedOpen(400L, 450L));
  }

  @Test
  void onSingleRead_positionOutsideAllRowGroupsWithoutLearnedColumns_schedulesNothing() {
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(900, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onSingleRead_positionOutsideAllRowGroupsWithLearnedDataColumn_schedulesNothing() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(900, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void
      onSingleRead_positionOutsideAllRowGroupsWithLearnedDictionaryColumn_prefetchesDictionaries() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(900, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges)
        .containsExactly(Range.closedOpen(150L, 170L), Range.closedOpen(450L, 470L))
        .inOrder();
  }

  @Test
  void onSingleRead_singleRowGroupWithLearnedDataAndDictionaryColumn_schedulesDictionaryOnly() {
    ParquetFileLayout singleRowGroupLayout =
        layout(rowGroup(0, dictionaryEncodedColumnChunk("category", 150, 50, 170)));
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "category");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    RowGroupPrefetchTracker singleRowGroupTracker =
        new RowGroupPrefetchTracker(
            singleRowGroupLayout,
            accessHistory,
            MAX_BLOCK_SIZE_BYTES,
            DictionaryTrigger.LAST_DICT_READ,
            (start, end) -> {});
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    singleRowGroupTracker.onSingleRead(900, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).containsExactly(Range.closedOpen(150L, 170L));
  }

  @Test
  void onBeforeRead_dictionaryPageReadWithLearnedColumns_schedulesDataRangesAndReturnsTrue() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    tracker.onSingleRead(900, 16, ranges -> true);
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    boolean handled = tracker.onBeforeRead(150, 8, scheduledRanges::addAll);

    assertThat(handled).isTrue();
    assertThat(scheduledRanges).containsExactly(Range.closedOpen(100L, 150L));
  }

  @Test
  void onBeforeRead_dataPageRead_schedulesNothingAndReturnsFalse() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    boolean handled = tracker.onBeforeRead(100, 16, scheduledRanges::addAll);

    assertThat(handled).isFalse();
    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onSingleRead_dataPageRead_prefetchesRemainingCurrentRowGroupAndNextRowGroupColumns() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "value");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(100, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges)
        .containsExactly(Range.closedOpen(250L, 300L), Range.closedOpen(400L, 450L))
        .inOrder();
  }

  @Test
  void onSingleRead_enteringLaterRowGroup_cancelsEarlierRowGroupWindow() {
    tracker.onSingleRead(100, 16, ranges -> true);

    tracker.onSingleRead(400, 16, ranges -> true);

    assertThat(cancelledWindows).containsExactly(Range.closedOpen(100L, 400L));
  }

  @Test
  void onSingleRead_splitMultiRowGroupFile_skipsFirstRowGroupAndNextRowGroupSpeculation() {
    ParquetFileLayout splitLayout =
        layout(
            rowGroup(0, columnChunk("id", 0, SPLIT_SIZE_BYTES)),
            rowGroup(1, columnChunk("id", SPLIT_SIZE_BYTES, SPLIT_SIZE_BYTES)));
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    RowGroupPrefetchTracker splitTracker =
        new RowGroupPrefetchTracker(
            splitLayout,
            accessHistory,
            MAX_BLOCK_SIZE_BYTES,
            DictionaryTrigger.LAST_DICT_READ,
            (start, end) -> {});
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    splitTracker.onSingleRead(SPLIT_SIZE_BYTES * 2, 16, scheduledRanges::addAll);
    splitTracker.onSingleRead(0, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onVectoredRead_rangeInsideRowGroup_recordsDataColumnAndNextRowGroupTarget() {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(100)
            .setLength(50)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    tracker.onVectoredRead(ImmutableList.of(range));

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("id");
    assertThat(tracker.pollPendingNextRowGroup(ranges -> true)).hasValue(1);
  }

  @Test
  void onVectoredRead_chunkReadAfterDictionarySweep_returnsNextRowGroup() {
    RowGroupPrefetchTracker sweepTracker =
        new RowGroupPrefetchTracker(
            layout(
                sweptRowGroup(0, 100),
                sweptRowGroup(1, 300),
                sweptRowGroup(2, 500),
                sweptRowGroup(3, 700)),
            accessHistory,
            MAX_BLOCK_SIZE_BYTES,
            DictionaryTrigger.LAST_DICT_READ,
            (start, end) -> {});
    for (long categoryDictionaryOffset : new long[] {200, 400, 600, 800}) {
      sweepTracker.onSingleRead(categoryDictionaryOffset, 20, ranges -> true);
    }
    GcsObjectRange chunkRead =
        GcsObjectRange.builder()
            .setOffset(100)
            .setLength(150)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    sweepTracker.onVectoredRead(ImmutableList.of(chunkRead));

    assertThat(sweepTracker.pollPendingNextRowGroup(ranges -> true)).hasValue(1);
  }

  @Test
  void onVectoredRead_splitMultiRowGroupFileAfterDictionarySweep_returnsNextRowGroup() {
    long secondRowGroupStart = SPLIT_SIZE_BYTES + 100;
    RowGroupPrefetchTracker splitTracker =
        new RowGroupPrefetchTracker(
            layout(splitSizedRowGroup(0, 0), splitSizedRowGroup(1, secondRowGroupStart)),
            accessHistory,
            MAX_BLOCK_SIZE_BYTES,
            DictionaryTrigger.LAST_DICT_READ,
            (start, end) -> {});
    splitTracker.onSingleRead(0, 8, ranges -> true);
    splitTracker.onSingleRead(secondRowGroupStart, 8, ranges -> true);
    GcsObjectRange chunkRead =
        GcsObjectRange.builder()
            .setOffset(100)
            .setLength(16)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    splitTracker.onVectoredRead(ImmutableList.of(chunkRead));

    assertThat(splitTracker.pollPendingNextRowGroup(ranges -> true)).hasValue(1);
  }

  @Test
  void pollPendingNextRowGroup_withoutPriorDataRead_schedulesNothingWhenOnlyDataColumnLearned() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    assertThat(tracker.pollPendingNextRowGroup(scheduledRanges::addAll)).isEmpty();
    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void pollPendingNextRowGroup_withoutPriorDataRead_prefetchesDictionariesAndReturnsEmpty() {
    accessHistory.recordDictionaryAccess(SCHEMA_FINGERPRINT, "category");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    assertThat(tracker.pollPendingNextRowGroup(scheduledRanges::addAll)).isEmpty();
    assertThat(scheduledRanges)
        .containsExactly(Range.closedOpen(150L, 170L), Range.closedOpen(450L, 470L))
        .inOrder();
  }

  @Test
  void onSingleRead_readBeforeLastObservedDataPage_observesNewPosition() {
    tracker.onSingleRead(170, 16, ranges -> true);

    tracker.onSingleRead(100, 16, ranges -> true);

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("category", "id");
  }

  @Test
  void onSingleRead_readInGapBetweenColumns_schedulesNothing() {
    accessHistory.recordDataAccess(SCHEMA_FINGERPRINT, "id");
    List<Range<Long>> scheduledRanges = new ArrayList<>();

    tracker.onSingleRead(210, 16, scheduledRanges::addAll);

    assertThat(scheduledRanges).isEmpty();
  }

  @Test
  void onVectoredRead_rangeOutsideAllRowGroups_recordsNothing() {
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(900)
            .setLength(16)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    tracker.onVectoredRead(ImmutableList.of(range));

    assertThat(accessHistory.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void onSingleRead_dataPageReadWhileFooterGateClosed_reopensGateBeforeClose() {
    recordOutcomes(FileFilterOutcome.FOOTER_REJECTED, FileFilterOutcome.FOOTER_REJECTED);
    recordOutcomes(FileFilterOutcome.SURVIVED);

    tracker.onSingleRead(100, 16, ranges -> true);

    assertThat(accessHistory.shouldSpeculateAtFooter(SCHEMA_FINGERPRINT)).isTrue();
  }

  @Test
  void onVectoredRead_dataPageReadWhileFooterGateClosed_reopensGateBeforeClose() {
    recordOutcomes(FileFilterOutcome.FOOTER_REJECTED, FileFilterOutcome.FOOTER_REJECTED);
    recordOutcomes(FileFilterOutcome.SURVIVED);
    GcsObjectRange range =
        GcsObjectRange.builder()
            .setOffset(100)
            .setLength(50)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();

    tracker.onVectoredRead(ImmutableList.of(range));

    assertThat(accessHistory.shouldSpeculateAtFooter(SCHEMA_FINGERPRINT)).isTrue();
  }

  @Test
  void recordOutcomeOnClose_afterDataPageRead_doesNotRecordRejection() {
    recordOutcomes(FileFilterOutcome.FOOTER_REJECTED, FileFilterOutcome.FOOTER_REJECTED);
    recordOutcomes(FileFilterOutcome.SURVIVED);
    tracker.onSingleRead(100, 16, ranges -> true);

    tracker.recordOutcomeOnClose();

    assertThat(accessHistory.shouldSpeculateAtFooter(SCHEMA_FINGERPRINT)).isTrue();
  }

  private void recordOutcomes(FileFilterOutcome... outcomes) {
    for (FileFilterOutcome outcome : outcomes) {
      accessHistory.recordFileOutcome(SCHEMA_FINGERPRINT, outcome);
    }
  }

  private static ParquetRowGroup sweptRowGroup(int index, long startOffset) {
    return rowGroup(
        index,
        columnChunk("id", startOffset, 50),
        dictionaryEncodedColumnChunk("name", startOffset + 50, 50, startOffset + 70),
        dictionaryEncodedColumnChunk("category", startOffset + 100, 50, startOffset + 120));
  }

  private static ParquetRowGroup splitSizedRowGroup(int index, long startOffset) {
    return rowGroup(
        index,
        dictionaryEncodedColumnChunk("category", startOffset, 100, startOffset + 20),
        columnChunk("id", startOffset + 100, SPLIT_SIZE_BYTES));
  }
}
