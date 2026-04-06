/*
 * Copyright 2025 Google LLC
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

/** Holds the state and logic for adaptive range read strategy. */
class AdaptiveReadStrategy {

  private final GcsReadOptions readOptions;
  private final FileAccessPattern fileAccessPattern;
  
  private boolean randomAccess;
  private long lastAdaptiveReadSessionEnd = -1;
  private int sequentialReadSessionCount = 0;

  AdaptiveReadStrategy(GcsReadOptions readOptions) {
    this.readOptions = readOptions;
    this.fileAccessPattern = readOptions.getFileAccessPattern();
    this.randomAccess = this.fileAccessPattern == FileAccessPattern.RANDOM;
  }

  long calculateAdaptiveReadSessionEnd(
      long readSessionPosition, long bytesToRead, long objectSize) {
    long endPosition = objectSize;
    if (randomAccess) {
      endPosition = readSessionPosition + Math.max(bytesToRead, readOptions.getMinRangeRequestSize());
    }
    long newEndPosition = Math.min(endPosition, objectSize);
    lastAdaptiveReadSessionEnd = newEndPosition;

    return newEndPosition;
  }

  void detectSequentialAccess(long readChannelPosition) {
    sequentialReadSessionCount =
        (readChannelPosition == lastAdaptiveReadSessionEnd) ? sequentialReadSessionCount + 1 : 0;
    if (!shouldDetectSequentialAccess()) {
      return;
    }
    randomAccess = false;
  }

  void detectRandomAccess(long readChannelPosition, long readSessionPosition) {
    if (!shouldDetectRandomAccess()) {
      return;
    }
    if (readChannelPosition < readSessionPosition) {
      randomAccess = true;
    } else if (readSessionPosition >= 0
        && readSessionPosition + readOptions.getInplaceSeekLimit() < readChannelPosition) {
      randomAccess = true;
    }
  }

  boolean shouldSeekInPlace(long channelPosition, long sessionPosition, long sessionEnd) {
    long seekDistance = channelPosition - sessionPosition;
    return seekDistance > 0
        && seekDistance <= readOptions.getInplaceSeekLimit()
        && channelPosition < sessionEnd;
  }

  boolean shouldDetectRandomAccess() {
    return !randomAccess && fileAccessPattern == FileAccessPattern.AUTO;
  }

  boolean shouldDetectSequentialAccess() {
    return randomAccess
        && fileAccessPattern == FileAccessPattern.AUTO
        && sequentialReadSessionCount >= readOptions.getSequentialReadSessionThreshold();
  }

  boolean isRandomAccess() {
    return randomAccess;
  }
}
