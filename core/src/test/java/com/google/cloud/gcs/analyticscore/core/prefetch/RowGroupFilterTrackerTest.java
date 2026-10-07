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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class RowGroupFilterTrackerTest {

  private static final String FILTER_COLUMN = "status";
  private static final String SECOND_FILTER_COLUMN = "region";

  @Test
  void hasReadAllDictionaries_secondKnownColumnHasNoDictionaryPage_returnsTrue() {
    ParquetColumnChunk dictChunk =
        ParquetColumnChunk.builder()
            .setColumnPath(FILTER_COLUMN)
            .setStartOffset(0L)
            .setDataPageOffset(100L)
            .setDictionaryPageOffset(0L)
            .setCompressedSize(500L)
            .build();
    ParquetColumnChunk plainChunk =
        ParquetColumnChunk.builder()
            .setColumnPath(SECOND_FILTER_COLUMN)
            .setStartOffset(500L)
            .setDataPageOffset(500L)
            .setCompressedSize(500L)
            .build();
    ParquetRowGroup rowGroup =
        ParquetRowGroup.builder()
            .setIndex(0)
            .addColumnChunk(dictChunk)
            .addColumnChunk(plainChunk)
            .build();
    ParquetFileLayout layout = ParquetFileLayout.create(12345, ImmutableList.of(rowGroup));
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);

    boolean allRead =
        tracker.hasReadAllDictionaries(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(allRead).isTrue();
  }

  @Test
  void hasReadAnyDictionary_oneOfTwoKnownColumnsRead_returnsTrue() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);

    boolean anyRead =
        tracker.hasReadAnyDictionary(0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(anyRead).isTrue();
  }

  @Test
  void hasReadAnyDictionary_onlyOtherRowGroupRead_returnsFalse() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(1, FILTER_COLUMN);

    boolean anyRead = tracker.hasReadAnyDictionary(0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(anyRead).isFalse();
  }

  @Test
  void hasReadAnyDictionary_onlyUnknownColumnRead_returnsFalse() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, SECOND_FILTER_COLUMN);

    boolean anyRead = tracker.hasReadAnyDictionary(0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(anyRead).isFalse();
  }

  @Test
  void getLastDataReadIndex_noDataRead_returnsMinusOne() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();

    assertThat(tracker.getLastDataReadIndex()).isEqualTo(-1);
  }

  @Test
  void getLastDataReadIndex_dataReadOutOfOrder_returnsHighestIndex() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDataRead(2);
    tracker.recordDataRead(0);

    assertThat(tracker.getLastDataReadIndex()).isEqualTo(2);
  }

  @Test
  void findNextSurvivingRowGroup_skipsRowGroupNotInUpfrontDictionarySweep() {
    ParquetFileLayout layout = createStatusLayout(4);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(3, FILTER_COLUMN);
    tracker.recordDataRead(0);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(2));
  }

  @Test
  void findNextSurvivingRowGroup_noFilterActivity_returnsEmpty() {
    ParquetFileLayout layout = createStatusLayout(2);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDataRead(0);

    OptionalInt nextIndex = tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of());

    assertThat(nextIndex).isEmpty();
  }

  @Test
  void findNextSurvivingRowGroup_rowGroupWithoutDictionaryPage_isNotSkippedBySweep() {
    ParquetFileLayout layout = createStatusLayout(4, /* plainEncodedIndices...= */ 1);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(3, FILTER_COLUMN);
    tracker.recordDataRead(0);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void findNextSurvivingRowGroup_knownColumnWithoutDictionaryPage_isNotSkippedBySweep() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainIndex= */ OptionalInt.of(1));
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(0);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void findNextSurvivingRowGroup_partialDictionarySweepInMultiColumnFilter_skipsRowGroup() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainIndex= */ OptionalInt.empty());
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(0, SECOND_FILTER_COLUMN);
    tracker.recordDictionaryRead(1, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, SECOND_FILTER_COLUMN);
    tracker.recordDataRead(0);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(2));
  }

  @Test
  void
      findNextSurvivingRowGroup_staleKnownFilterColumnNeverReadInCurrentFile_doesNotSkipSurvivingRowGroup() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainIndex= */ OptionalInt.empty());
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(1, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(0);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void
      findNextSurvivingRowGroup_candidateBeyondMaxDictionaryIndexWithUnreadDictionary_returnsEmpty() {
    ParquetFileLayout layout = createStatusLayout(9);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    for (int index = 0; index <= 4; index++) {
      tracker.recordDictionaryRead(index, FILTER_COLUMN);
    }
    tracker.recordDataRead(4);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(layout, 4, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextIndex).isEmpty();
  }

  @Test
  void
      findNextSurvivingRowGroup_plainRowGroupAfterSkippedCandidateBeyondMaxDictionaryIndex_returnsEmpty() {
    ParquetFileLayout layout = createStatusLayout(9, /* plainEncodedIndices...= */ 6);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    for (int index = 0; index <= 4; index++) {
      tracker.recordDictionaryRead(index, FILTER_COLUMN);
    }
    tracker.recordDataRead(4);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(layout, 4, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextIndex).isEmpty();
  }

  @Test
  void
      findNextSurvivingRowGroup_plainRowGroupImmediatelyAfterMaxDictionaryIndex_returnsPlainRowGroup() {
    ParquetFileLayout layout = createStatusLayout(9, /* plainEncodedIndices...= */ 4);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    for (int index = 0; index <= 3; index++) {
      tracker.recordDictionaryRead(index, FILTER_COLUMN);
    }
    tracker.recordDataRead(3);

    OptionalInt nextIndex =
        tracker.findNextSurvivingRowGroup(layout, 3, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextIndex).isEqualTo(OptionalInt.of(4));
  }

  /**
   * Builds a layout with one {@code status} column per row group, dictionary encoded except in
   * {@code plainEncodedIndices}.
   */
  private static ParquetFileLayout createStatusLayout(
      int rowGroupCount, int... plainEncodedIndices) {
    ImmutableSet<Integer> plainEncoded =
        Arrays.stream(plainEncodedIndices).boxed().collect(ImmutableSet.toImmutableSet());
    ImmutableList.Builder<ParquetRowGroup> rowGroups = ImmutableList.builder();
    for (int index = 0; index < rowGroupCount; index++) {
      ParquetColumnChunk.Builder chunk =
          ParquetColumnChunk.builder()
              .setColumnPath(FILTER_COLUMN)
              .setStartOffset(index * 1000L)
              .setDataPageOffset(index * 1000L + 100L)
              .setCompressedSize(500L);
      if (!plainEncoded.contains(index)) {
        chunk.setDictionaryPageOffset(index * 1000L);
      }
      rowGroups.add(
          ParquetRowGroup.builder().setIndex(index).addColumnChunk(chunk.build()).build());
    }
    return ParquetFileLayout.create(12345, rowGroups.build());
  }

  private static ParquetFileLayout createTwoFilterColumnLayout(
      int rowGroupCount, OptionalInt secondColumnPlainIndex) {
    ImmutableList.Builder<ParquetRowGroup> rowGroups = ImmutableList.builder();
    for (int index = 0; index < rowGroupCount; index++) {
      long baseOffset = index * 2000L;
      ParquetColumnChunk firstChunk =
          ParquetColumnChunk.builder()
              .setColumnPath(FILTER_COLUMN)
              .setStartOffset(baseOffset)
              .setDataPageOffset(baseOffset + 100L)
              .setDictionaryPageOffset(baseOffset)
              .setCompressedSize(500L)
              .build();
      ParquetColumnChunk.Builder secondChunk =
          ParquetColumnChunk.builder()
              .setColumnPath(SECOND_FILTER_COLUMN)
              .setStartOffset(baseOffset + 500L)
              .setDataPageOffset(baseOffset + 600L)
              .setCompressedSize(500L);
      if (secondColumnPlainIndex.orElse(-1) != index) {
        secondChunk.setDictionaryPageOffset(baseOffset + 500L);
      }
      rowGroups.add(
          ParquetRowGroup.builder()
              .setIndex(index)
              .addColumnChunk(firstChunk)
              .addColumnChunk(secondChunk.build())
              .build());
    }
    return ParquetFileLayout.create(12345, rowGroups.build());
  }
}
