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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SchemaAccessHistoryTest {

  private static final int SCHEMA_FINGERPRINT = 12345;
  private static final int OTHER_SCHEMA_FINGERPRINT = 67890;
  private static final int MAX_COLUMNS = 3;

  @Test
  void getDataColumns_unknownSchema_returnsEmpty() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    var dataColumns = history.getDataColumns(SCHEMA_FINGERPRINT);

    assertThat(dataColumns).isEmpty();
  }

  @Test
  void getSharedInstance_sameCapacity_returnsTheSameInstance() {
    SchemaAccessHistory history = SchemaAccessHistory.getSharedInstance(MAX_COLUMNS);

    SchemaAccessHistory sameHistory = SchemaAccessHistory.getSharedInstance(MAX_COLUMNS);

    assertThat(sameHistory).isSameInstanceAs(history);
  }

  @Test
  void getSharedInstance_differentCapacity_returnsADifferentInstance() {
    SchemaAccessHistory history = SchemaAccessHistory.getSharedInstance(MAX_COLUMNS);

    SchemaAccessHistory otherHistory = SchemaAccessHistory.getSharedInstance(MAX_COLUMNS + 1);

    assertThat(otherHistory).isNotSameInstanceAs(history);
  }

  @Test
  void getSharedInstance_nonPositiveCapacity_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> SchemaAccessHistory.getSharedInstance(0));
  }

  @Test
  void recordDataAccess_singleColumn_isReturned() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("customer_id");
  }

  @Test
  void recordDataAccess_sameColumnTwice_isRecordedOnce() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");
    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).hasSize(1);
  }

  @Test
  void recordDataAccess_differentSchemas_areIsolated() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    assertThat(history.getDataColumns(OTHER_SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void recordDataAccess_beyondMaxColumns_stopsTracking() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    recordDataColumns(history, "a", "b", "c", "d", "e");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).hasSize(MAX_COLUMNS);
  }

  @Test
  void recordDictionaryAccess_singleColumn_isReturned() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
  }

  @Test
  void recordDictionaryAccess_doesNotAppearAsDataColumn() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);

    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void recordDataAccess_afterDictionaryAccess_promotesColumnToDataColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);
    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    history.recordDataAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
  }

  @Test
  void recordDataAccess_afterDictionaryAccess_removesColumnFromDictionaryColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);
    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    history.recordDataAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDictionaryColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void recordDictionaryAccess_afterDataAccess_doesNotDemoteColumn() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);
    history.recordDataAccess(SCHEMA_FINGERPRINT, "status");

    history.recordDictionaryAccess(SCHEMA_FINGERPRINT, "status");

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).containsExactly("status");
  }

  @Test
  void invalidateAll_discardsRecordedColumns() {
    SchemaAccessHistory history = new SchemaAccessHistory(MAX_COLUMNS);
    history.recordDataAccess(SCHEMA_FINGERPRINT, "customer_id");

    history.invalidateAll();

    assertThat(history.getDataColumns(SCHEMA_FINGERPRINT)).isEmpty();
  }

  @Test
  void constructor_nonPositiveMaxColumns_throws() {
    assertThrows(IllegalArgumentException.class, () -> new SchemaAccessHistory(0));
  }

  private static void recordDataColumns(SchemaAccessHistory history, String... columnPaths) {
    for (String columnPath : columnPaths) {
      history.recordDataAccess(SCHEMA_FINGERPRINT, columnPath);
    }
  }
}
