/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.cloud.gcs.analyticscore.client.RangeSplitter.Part;
import org.junit.jupiter.api.Test;

class RangeSplitterTest {

  @Test
  void split_lengthEqualToMaxPartSize_returnsOnePart() {
    assertThat(RangeSplitter.split(8, 8)).containsExactly(Part.create(0, 8));
  }

  @Test
  void split_lengthJustAboveMaxPartSize_returnsTwoNearEqualParts() {
    assertThat(RangeSplitter.split(9, 8)).containsExactly(Part.create(0, 5), Part.create(5, 4));
  }

  @Test
  void split_unevenLength_partsDifferByAtMostOneByte() {
    assertThat(RangeSplitter.split(20, 8))
        .containsExactly(Part.create(0, 7), Part.create(7, 7), Part.create(14, 6))
        .inOrder();
  }

  @Test
  void split_exactMultipleOfMaxPartSize_returnsFullSizeParts() {
    assertThat(RangeSplitter.split(24, 8))
        .containsExactly(Part.create(0, 8), Part.create(8, 8), Part.create(16, 8))
        .inOrder();
  }

  @Test
  void split_zeroLength_returnsNoParts() {
    assertThat(RangeSplitter.split(0, 8)).isEmpty();
  }

  @Test
  void partCount_lengthAboveMaxPartSize_roundsUp() {
    assertThat(RangeSplitter.partCount(17, 8)).isEqualTo(3);
  }

  @Test
  void partCount_negativeLength_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> RangeSplitter.partCount(-1, 8));

    assertThat(e).hasMessageThat().isEqualTo("length must not be negative: -1");
  }

  @Test
  void partCount_nonPositiveMaxPartSize_throwsIllegalArgumentException() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> RangeSplitter.partCount(8, 0));

    assertThat(e).hasMessageThat().isEqualTo("maxPartSize must be positive: 0");
  }
}
