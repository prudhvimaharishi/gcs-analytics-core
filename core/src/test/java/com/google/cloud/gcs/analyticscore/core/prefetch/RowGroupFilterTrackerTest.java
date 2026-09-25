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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.OptionalInt;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class RowGroupFilterTrackerTest {

  private static final String FILTER_COLUMN = "status";
  private static final String SECOND_FILTER_COLUMN = "region";

  @Test
  void hasReadAllDictionaries_secondKnownColumnHasNoDictionaryPage_returnsTrue() {
    ParquetColumnChunk dictChunk =
        ParquetColumnChunk.builder()
            .setColumnPath(FILTER_COLUMN)
            .setRowGroupOrdinal(0)
            .setStartOffset(0L)
            .setDataPageOffset(100L)
            .setDictionaryPageOffset(0L)
            .setCompressedSize(500L)
            .build();
    ParquetColumnChunk plainChunk =
        ParquetColumnChunk.builder()
            .setColumnPath(SECOND_FILTER_COLUMN)
            .setRowGroupOrdinal(0)
            .setStartOffset(500L)
            .setDataPageOffset(500L)
            .setCompressedSize(500L)
            .build();
    ParquetRowGroup rowGroup =
        ParquetRowGroup.builder()
            .setOrdinal(0)
            .setRowCount(100L)
            .setColumnChunks(
                ImmutableMap.of(FILTER_COLUMN, dictChunk, SECOND_FILTER_COLUMN, plainChunk))
            .build();
    ParquetFileLayout layout = ParquetFileLayout.create(12345, ImmutableList.of(rowGroup), 2000L);
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
  void getLastDataReadOrdinal_noDataRead_returnsMinusOne() {
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();

    assertThat(tracker.getLastDataReadOrdinal()).isEqualTo(-1);
  }

  @Test
  void getLastDataReadOrdinal_dataReadOutOfOrder_returnsHighestOrdinal() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(new String[][] {{"A", "B"}, {"C", "D"}, {"E", "F"}});
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDataRead(layout, 2, ImmutableSet.of());
    tracker.recordDataRead(layout, 0, ImmutableSet.of());

    assertThat(tracker.getLastDataReadOrdinal()).isEqualTo(2);
  }

  @Test
  void findNextSurvivingRowGroup_skipsRowGroupNotInUpfrontDictionarySweep() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(
            new String[][] {{"A", "Z"}, {"A", "Z"}, {"A", "Z"}, {"A", "Z"}});
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(3, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(2));
  }

  @Test
  void findNextSurvivingRowGroup_skipsRowGroupMatchingRejectedStatistics() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(
            new String[][] {
              {"ACTIVE", "COMPLETED"},
              {"PENDING", "SHIPPED"},
              {"ACTIVE", "DELIVERED"},
              {"PROCESSING", "READY"}
            });
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN));
    tracker.recordDataRead(layout, 2, ImmutableSet.of(FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(layout, 2, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.empty());
  }

  @Test
  void
      findNextSurvivingRowGroup_dictionaryEvaluatedSkippedRowGroup_doesNotRejectLaterMatchingStatistics() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(
            new String[][] {
              {"ACTIVE", "COMPLETED"},
              {"PENDING", "SHIPPED"},
              {"ACTIVE", "DELIVERED"},
              {"PROCESSING", "READY"}
            });
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(1, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN));
    tracker.recordDataRead(layout, 2, ImmutableSet.of(FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(layout, 2, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(3));
  }

  @Test
  void findNextSurvivingRowGroup_noFilterActivity_returnsAdjacentRowGroup() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(new String[][] {{"A", "B"}, {"C", "D"}});
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDataRead(layout, 0, ImmutableSet.of());

    OptionalInt nextOrdinal = tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of());

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void findNextSurvivingRowGroup_rowGroupWithoutDictionaryPage_isNotSkippedBySweep() {
    ParquetFileLayout layout =
        createLayoutWithStatusRanges(
            new String[][] {{"A", "Z"}, {"A", "Z"}, {"A", "Z"}, {"A", "Z"}},
            /* plainEncodedOrdinals...= */ 1);
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(3, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(layout, 0, ImmutableSet.of(FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void findNextSurvivingRowGroup_knownColumnWithoutDictionaryPage_isNotSkippedBySweep() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainOrdinal= */ OptionalInt.of(1));
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(1));
  }

  @Test
  void findNextSurvivingRowGroup_partialDictionarySweepInMultiColumnFilter_skipsRowGroup() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainOrdinal= */ OptionalInt.empty());
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(0, SECOND_FILTER_COLUMN);
    tracker.recordDictionaryRead(1, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, SECOND_FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(2));
  }

  @Test
  void
      findNextSurvivingRowGroup_staleKnownFilterColumnNeverReadInCurrentFile_doesNotSkipSurvivingRowGroup() {
    ParquetFileLayout layout =
        createTwoFilterColumnLayout(
            /* rowGroupCount= */ 3, /* secondColumnPlainOrdinal= */ OptionalInt.empty());
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDictionaryRead(0, FILTER_COLUMN);
    tracker.recordDictionaryRead(1, FILTER_COLUMN);
    tracker.recordDictionaryRead(2, FILTER_COLUMN);
    tracker.recordDataRead(layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    OptionalInt nextOrdinal =
        tracker.findNextSurvivingRowGroup(
            layout, 0, ImmutableSet.of(FILTER_COLUMN, SECOND_FILTER_COLUMN));

    assertThat(nextOrdinal).isEqualTo(OptionalInt.of(1));
  }

  /**
   * Builds a layout with one {@code status} column per row group, dictionary encoded except in
   * {@code plainEncodedOrdinals}.
   */
  private static ParquetFileLayout createLayoutWithStatusRanges(
      String[][] minMaxPairs, int... plainEncodedOrdinals) {
    ImmutableSet<Integer> plainEncoded =
        Arrays.stream(plainEncodedOrdinals).boxed().collect(ImmutableSet.toImmutableSet());
    ImmutableList.Builder<ParquetRowGroup> rowGroups = ImmutableList.builder();
    for (int ordinal = 0; ordinal < minMaxPairs.length; ordinal++) {
      ParquetColumnStatistics stats =
          ParquetColumnStatistics.of(
              Type.BYTE_ARRAY,
              minMaxPairs[ordinal][0].getBytes(UTF_8),
              minMaxPairs[ordinal][1].getBytes(UTF_8));
      ParquetColumnChunk.Builder chunk =
          ParquetColumnChunk.builder()
              .setColumnPath(FILTER_COLUMN)
              .setRowGroupOrdinal(ordinal)
              .setStartOffset(ordinal * 1000L)
              .setDataPageOffset(ordinal * 1000L + 100L)
              .setCompressedSize(500L)
              .setStatistics(stats);
      if (!plainEncoded.contains(ordinal)) {
        chunk.setDictionaryPageOffset(ordinal * 1000L);
      }
      rowGroups.add(
          ParquetRowGroup.builder()
              .setOrdinal(ordinal)
              .setRowCount(100L)
              .setColumnChunks(ImmutableMap.of(FILTER_COLUMN, chunk.build()))
              .build());
    }
    return ParquetFileLayout.create(12345, rowGroups.build(), 10000L);
  }

  private static ParquetFileLayout createTwoFilterColumnLayout(
      int rowGroupCount, OptionalInt secondColumnPlainOrdinal) {
    ImmutableList.Builder<ParquetRowGroup> rowGroups = ImmutableList.builder();
    for (int ordinal = 0; ordinal < rowGroupCount; ordinal++) {
      long baseOffset = ordinal * 2000L;
      ParquetColumnChunk firstChunk =
          ParquetColumnChunk.builder()
              .setColumnPath(FILTER_COLUMN)
              .setRowGroupOrdinal(ordinal)
              .setStartOffset(baseOffset)
              .setDataPageOffset(baseOffset + 100L)
              .setDictionaryPageOffset(baseOffset)
              .setCompressedSize(500L)
              .build();
      ParquetColumnChunk.Builder secondChunk =
          ParquetColumnChunk.builder()
              .setColumnPath(SECOND_FILTER_COLUMN)
              .setRowGroupOrdinal(ordinal)
              .setStartOffset(baseOffset + 500L)
              .setDataPageOffset(baseOffset + 600L)
              .setCompressedSize(500L);
      if (secondColumnPlainOrdinal.orElse(-1) != ordinal) {
        secondChunk.setDictionaryPageOffset(baseOffset + 500L);
      }
      rowGroups.add(
          ParquetRowGroup.builder()
              .setOrdinal(ordinal)
              .setRowCount(100L)
              .setColumnChunks(
                  ImmutableMap.of(
                      FILTER_COLUMN, firstChunk, SECOND_FILTER_COLUMN, secondChunk.build()))
              .build());
    }
    return ParquetFileLayout.create(12345, rowGroups.build(), 10000L);
  }
}
