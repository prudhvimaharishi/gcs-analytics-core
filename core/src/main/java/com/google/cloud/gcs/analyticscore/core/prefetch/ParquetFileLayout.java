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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.Optional;

/**
 * The decoded byte layout of a Parquet object.
 *
 * <p>Describes the row groups and column chunks recorded in the file footer.
 */
@AutoValue
abstract class ParquetFileLayout {

  /** Returns a 32-bit fingerprint identifying the schema, stable across files. */
  abstract int getSchemaFingerprint();

  /** Returns the row groups of this file, ordered by ordinal. */
  abstract ImmutableList<ParquetRowGroup> getRowGroups();

  /** Creates a layout from the row groups described by the file footer. */
  static ParquetFileLayout create(int schemaFingerprint, ImmutableList<ParquetRowGroup> rowGroups) {
    return new AutoValue_ParquetFileLayout(schemaFingerprint, rowGroups);
  }

  /** Returns the row group with the given ordinal, if it exists. */
  final Optional<ParquetRowGroup> getRowGroup(int ordinal) {
    if (ordinal < 0 || ordinal >= getRowGroups().size()) {
      return Optional.empty();
    }
    return Optional.of(getRowGroups().get(ordinal));
  }
}
