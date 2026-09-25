/*
 * Copyright 2025 Google LLC
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

public interface VectoredSeekableByteChannel extends SeekableByteChannel {
  /**
   * Reads the list of provided ranges in parallel.
   *
   * @param ranges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException on any IO failure
   */
  void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException;

  /**
   * Moves the position to {@code newPosition} after the bytes in between were consumed from
   * somewhere else, such as a prefetch cache.
   *
   * <p>Implementations that adapt to the read pattern use this to tell a cache hit apart from a
   * seek: both leave the channel further along, but only one of them means the reader jumped.
   *
   * @param newPosition the position the reader has reached
   * @throws IOException on any IO failure
   */
  default void advanceAfterExternalRead(long newPosition) throws IOException {
    position(newPosition);
  }

  /** Returns the item metadata info if available, or null. */
  @Nullable
  default GcsItemInfo getItemInfo() {
    return null;
  }
}
