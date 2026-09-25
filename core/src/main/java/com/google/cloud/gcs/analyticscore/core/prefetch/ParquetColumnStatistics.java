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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.UnsignedBytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.parquet.format.Type;

/**
 * Immutable min/max statistics for a single Parquet column chunk.
 *
 * <p>Provides type-aware range containment checks so that a row group whose filter column values
 * fall entirely within a previously rejected range can be identified without re-evaluating SQL
 * predicates.
 */
final class ParquetColumnStatistics {

  private final Type type;
  private final byte[] minBytes;
  private final byte[] maxBytes;

  private ParquetColumnStatistics(Type type, byte[] minBytes, byte[] maxBytes) {
    this.type = checkNotNull(type, "type cannot be null");
    this.minBytes = checkNotNull(minBytes, "minBytes cannot be null").clone();
    this.maxBytes = checkNotNull(maxBytes, "maxBytes cannot be null").clone();
  }

  /** Creates a statistics instance from raw footer min/max bytes. */
  static ParquetColumnStatistics of(Type type, byte[] minBytes, byte[] maxBytes) {
    return new ParquetColumnStatistics(type, minBytes, maxBytes);
  }

  private static int toInt(byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  private static long toLong(byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  private static int compareLexicographically(byte[] left, byte[] right) {
    return UnsignedBytes.lexicographicalComparator().compare(left, right);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ParquetColumnStatistics)) {
      return false;
    }
    ParquetColumnStatistics that = (ParquetColumnStatistics) obj;
    return this.type == that.type
        && Arrays.equals(this.minBytes, that.minBytes)
        && Arrays.equals(this.maxBytes, that.maxBytes);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + Arrays.hashCode(minBytes);
    result = 31 * result + Arrays.hashCode(maxBytes);
    return result;
  }
}
