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

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;

class SchemaAccessHistoryTest {

  private static final int SCHEMA_FINGERPRINT = 12345;
  private static final int OTHER_SCHEMA_FINGERPRINT = 67890;

  @Test
  void getDataColumns_unknownSchema_returnsEmpty() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    var dataColumns = history.getDataColumns(SCHEMA_FINGERPRINT);

    assertThat(dataColumns).isEmpty();
  }

  @Test
  void recordDataAccess_singleColumn_isReturned() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("customer_id");
  }

  @Test
  void recordDataAccess_sameColumnTwice_isRecordedOnce() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");
    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).hasSize(1);
  }

  @Test
  void recordDataAccess_differentSchemas_areIsolated() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(OTHER_SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void recordDataAccess_beyondMaxColumns_keepsSizeAtMax() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    recordColumnsOnce(history, "column_", SchemaAccessHistory.MAX_COLUMNS_PER_SCHEMA + 50);

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT))
        .hasSize(SchemaAccessHistory.MAX_COLUMNS_PER_SCHEMA);
  }

  @Test
  void recordDataAccess_frequentColumnBeyondMax_survivesRareColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory();
    recordColumnsOnce(history, "cold_", SchemaAccessHistory.MAX_COLUMNS_PER_SCHEMA);
    for (int i = 0; i < 10; i++) {
      history.recordDataAccess(SCHEMA_FINGERPRINT, "price");
    }

    recordColumnsOnce(history, "rare_", 50);

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).contains("price");
  }

  @Test
  void getDictionaryColumns_unknownSchema_returnsEmpty() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    var dictionaryColumns = history.getDictionaryColumns(SCHEMA_FINGERPRINT);

    assertThat(dictionaryColumns).isEmpty();
  }

  @Test
  void recordDictionaryAccess_singleColumn_isReturnedOnlyAsDictionaryColumn() {
    SchemaAccessHistory history = new SchemaAccessHistory();

    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void recordDataAccess_afterDictionaryAccess_retainsColumnInDictionaryColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory();
    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    history.recordDataAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
  }

  @Test
  void recordDictionaryAccess_afterDataAccess_recordsInBothSets() {
    SchemaAccessHistory history = new SchemaAccessHistory();
    history.recordDataAccess(SCHEMA_FINGERPRINT, "status");

    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
  }

  @Test
  void invalidateAll_discardsRecordedColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory();
    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");
    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    history.invalidateAll();

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  private static void recordColumnsOnce(SchemaAccessHistory history, String prefix, int count) {
    for (int i = 0; i < count; i++) {
      history.recordDataAccess(SCHEMA_FINGERPRINT, prefix + i);
    }
  }
}
