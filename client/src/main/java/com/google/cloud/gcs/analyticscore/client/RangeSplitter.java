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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.math.RoundingMode;

/** Splits a byte range into contiguous, near-equal parts no larger than a maximum part size. */
final class RangeSplitter {

  private RangeSplitter() {}

  /** A part of a split range. Its offset is relative to the start of the range. */
  @AutoValue
  abstract static class Part {

    abstract int getOffset();

    abstract int getLength();

    static Part create(int offset, int length) {
      return new AutoValue_RangeSplitter_Part(offset, length);
    }
  }

  /** Returns how many parts a range of {@code length} bytes is split into. */
  static int partCount(int length, int maxPartSize) {
    checkArgument(length >= 0, "length must not be negative: %s", length);
    checkArgument(maxPartSize > 0, "maxPartSize must be positive: %s", maxPartSize);
    return IntMath.divide(length, maxPartSize, RoundingMode.CEILING);
  }

  /**
   * Returns the parts of a range of {@code length} bytes, in order. Part sizes differ by at most
   * one byte, because a split read is only as fast as its slowest part.
   */
  static ImmutableList<Part> split(int length, int maxPartSize) {
    int partCount = partCount(length, maxPartSize);
    ImmutableList.Builder<Part> parts = ImmutableList.builderWithExpectedSize(partCount);
    int offset = 0;
    for (int index = 0; index < partCount; index++) {
      int partLength = length / partCount + (index < length % partCount ? 1 : 0);
      parts.add(Part.create(offset, partLength));
      offset += partLength;
    }
    return parts.build();
  }
}
