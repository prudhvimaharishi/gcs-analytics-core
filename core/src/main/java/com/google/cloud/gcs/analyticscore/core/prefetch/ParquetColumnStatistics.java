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
import java.util.OptionalInt;
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

  /**
   * Returns whether every value in this column chunk's {@code [min, max]} interval falls inside
   * {@code other}'s {@code [min, max]} interval. Returns {@code false} whenever the values cannot
   * be ordered, such as for {@code INT96}, {@code NaN}, or values of an unexpected width.
   */
  boolean isContainedIn(ParquetColumnStatistics other) {
    checkNotNull(other, "other cannot be null");
    if (this.type != other.type) {
      return false;
    }
    if (Arrays.equals(this.minBytes, other.minBytes)
        && Arrays.equals(this.maxBytes, other.maxBytes)) {
      return true;
    }
    OptionalInt minComparison = compareValues(this.minBytes, other.minBytes);
    OptionalInt maxComparison = compareValues(this.maxBytes, other.maxBytes);
    return minComparison.isPresent()
        && maxComparison.isPresent()
        && minComparison.getAsInt() >= 0
        && maxComparison.getAsInt() <= 0;
  }

  /** Compares two plain-encoded values of {@link #type}, or returns empty if they are unordered. */
  private OptionalInt compareValues(byte[] left, byte[] right) {
    switch (type) {
      case BOOLEAN:
        if (left.length == 1 && right.length == 1) {
          return OptionalInt.of(Integer.compare(left[0], right[0]));
        }
        return OptionalInt.empty();
      case INT32:
        if (left.length == Integer.BYTES && right.length == Integer.BYTES) {
          return OptionalInt.of(Integer.compare(toInt(left), toInt(right)));
        }
        return OptionalInt.empty();
      case INT64:
        if (left.length == Long.BYTES && right.length == Long.BYTES) {
          return OptionalInt.of(Long.compare(toLong(left), toLong(right)));
        }
        return OptionalInt.empty();
      case FLOAT:
        if (left.length == Float.BYTES && right.length == Float.BYTES) {
          return compareDoubles(
              Float.intBitsToFloat(toInt(left)), Float.intBitsToFloat(toInt(right)));
        }
        return OptionalInt.empty();
      case DOUBLE:
        if (left.length == Double.BYTES && right.length == Double.BYTES) {
          return compareDoubles(
              Double.longBitsToDouble(toLong(left)), Double.longBitsToDouble(toLong(right)));
        }
        return OptionalInt.empty();
      case BYTE_ARRAY:
      case FIXED_LEN_BYTE_ARRAY:
        return OptionalInt.of(compareLexicographically(left, right));
      default:
        return OptionalInt.empty();
    }
  }

  private static OptionalInt compareDoubles(double left, double right) {
    if (Double.isNaN(left) || Double.isNaN(right)) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(Double.compare(left, right));
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
