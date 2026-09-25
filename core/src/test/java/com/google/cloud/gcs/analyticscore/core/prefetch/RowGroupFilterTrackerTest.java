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

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class RowGroupFilterTrackerTest {

  private static final String FILTER_COLUMN = "status";
  private static final String SECOND_FILTER_COLUMN = "region";

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
    RowGroupFilterTracker tracker = new RowGroupFilterTracker();
    tracker.recordDataRead(2);
    tracker.recordDataRead(0);

    assertThat(tracker.getLastDataReadOrdinal()).isEqualTo(2);
  }
}
