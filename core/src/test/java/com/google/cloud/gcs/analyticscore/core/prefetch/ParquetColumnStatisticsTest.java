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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class ParquetColumnStatisticsTest {

  @Test
  void isContainedIn_stringRangeInsideRejectedRange_returnsTrue() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(
            Type.BYTE_ARRAY, "PENDING".getBytes(UTF_8), "SHIPPED".getBytes(UTF_8));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(
            Type.BYTE_ARRAY, "PROCESSING".getBytes(UTF_8), "READY".getBytes(UTF_8));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isTrue();
  }

  @Test
  void isContainedIn_stringRangeExtendingOutsideRejectedRange_returnsFalse() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(
            Type.BYTE_ARRAY, "PENDING".getBytes(UTF_8), "SHIPPED".getBytes(UTF_8));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(
            Type.BYTE_ARRAY, "CANCELLED".getBytes(UTF_8), "READY".getBytes(UTF_8));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isFalse();
  }

  @Test
  void isContainedIn_int32NegativeAndPositiveSignedBounds_returnsTrue() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.INT32, intBytes(-100), intBytes(50));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.INT32, intBytes(-20), intBytes(10));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isTrue();
  }

  @Test
  void isContainedIn_doubleRangeStrictlyInsideRejectedRange_returnsTrue() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.DOUBLE, doubleBytes(10.0), doubleBytes(50.0));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.DOUBLE, doubleBytes(20.0), doubleBytes(40.0));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isTrue();
  }

  @Test
  void isContainedIn_doubleRangeBelowRejectedMinWithSameMax_returnsFalse() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.DOUBLE, doubleBytes(10.0), doubleBytes(50.0));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.DOUBLE, doubleBytes(-999.0), doubleBytes(50.0));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isFalse();
  }

  @Test
  void isContainedIn_floatRangeInsideRejectedRange_returnsTrue() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.FLOAT, floatBytes(-5.5f), floatBytes(5.5f));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.FLOAT, floatBytes(-1.0f), floatBytes(1.0f));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isTrue();
  }

  @Test
  void isContainedIn_floatNaNBound_returnsFalse() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.FLOAT, floatBytes(Float.NaN), floatBytes(100.0f));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.FLOAT, floatBytes(1.0f), floatBytes(2.0f));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isFalse();
  }

  @Test
  void isContainedIn_booleanRangeWiderThanRejectedRange_returnsFalse() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.BOOLEAN, new byte[] {1}, new byte[] {1});
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(Type.BOOLEAN, new byte[] {0}, new byte[] {1});

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isFalse();
  }

  @Test
  void isContainedIn_int96DifferentBounds_returnsFalse() {
    ParquetColumnStatistics rejected =
        ParquetColumnStatistics.of(Type.INT96, new byte[12], filledBytes(12, (byte) 9));
    ParquetColumnStatistics candidate =
        ParquetColumnStatistics.of(
            Type.INT96, filledBytes(12, (byte) 1), filledBytes(12, (byte) 9));

    boolean contained = candidate.isContainedIn(rejected);

    assertThat(contained).isFalse();
  }

  private static byte[] intBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
  }

  private static byte[] floatBytes(float value) {
    return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
  }

  private static byte[] doubleBytes(double value) {
    return ByteBuffer.allocate(Double.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putDouble(value)
        .array();
  }

  private static byte[] filledBytes(int length, byte value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, value);
    return bytes;
  }
}
