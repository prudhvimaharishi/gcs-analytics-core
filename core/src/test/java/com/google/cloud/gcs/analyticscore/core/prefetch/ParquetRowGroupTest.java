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
import static com.google.cloud.gcs.analyticscore.core.prefetch.ParquetLayoutFixtures.rowGroup;
import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

public class ParquetRowGroupTest {

  @Test
  void getColumnChunk_knownColumnPath_returnsThatChunk() {
    ParquetColumnChunk price = columnChunk("price", 100, 50);
    ParquetRowGroup group = rowGroup(0, price, columnChunk("name", 150, 30));

    assertThat(group.getColumnChunk("price")).hasValue(price);
  }

  @Test
  void getColumnChunk_unknownColumnPath_returnsEmpty() {
    ParquetRowGroup group = rowGroup(0, columnChunk("price", 100, 50));

    assertThat(group.getColumnChunk("absent")).isEmpty();
  }

  @Test
  void getStartOffset_returnsLowestChunkStartOffset() {
    ParquetRowGroup group =
        rowGroup(0, columnChunk("price", 300, 50), columnChunk("name", 100, 50));

    long startOffset = group.getStartOffset();

    assertThat(startOffset).isEqualTo(100);
  }

  @Test
  void getEndOffset_returnsHighestChunkEndOffset() {
    ParquetRowGroup group =
        rowGroup(0, columnChunk("price", 300, 50), columnChunk("name", 100, 50));

    long endOffset = group.getEndOffset();

    assertThat(endOffset).isEqualTo(350);
  }

  @Test
  void getStartOffset_rowGroupWithoutChunks_returnsZero() {
    ParquetRowGroup group = rowGroup(0);

    long startOffset = group.getStartOffset();

    assertThat(startOffset).isEqualTo(0);
  }

  @Test
  void getEndOffset_rowGroupWithoutChunks_returnsZero() {
    ParquetRowGroup group = rowGroup(0);

    long endOffset = group.getEndOffset();

    assertThat(endOffset).isEqualTo(0);
  }

  @Test
  void addColumnChunk_keysTheChunkByItsColumnPath() {
    ParquetRowGroup group = rowGroup(0, columnChunk("address.city", 100, 50));

    assertThat(group.getColumnChunks().keySet()).containsExactly("address.city");
  }
}
