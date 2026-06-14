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

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.util.Collections;

class AdaptiveReadStrategy extends AbstractReadStrategy {
  private ReadStrategy currentStrategy;
  private boolean isRandomMode = false;
  private int sequentialReadCount = 0;
  private long lastReadEndPosition = 0;

  AdaptiveReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions options, GcsItemInfo itemInfo)
      throws IOException {
    this(
        storage,
        itemId,
        options,
        itemInfo,
        new Telemetry(com.google.common.collect.ImmutableList.of()));
  }

  AdaptiveReadStrategy(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions options,
      GcsItemInfo itemInfo,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemId, options, itemInfo, telemetry);
    this.isRandomMode =
        options.getFileAccessPattern() == FileAccessPattern.RANDOM
            || options.getFileAccessPattern() == FileAccessPattern.AUTO_RANDOM;
    this.currentStrategy =
        isRandomMode
            ? new RandomReadStrategy(storage, itemId, options, itemInfo, telemetry)
            : new SequentialReadStrategy(storage, itemId, options, itemInfo, telemetry);
  }

  @Override
  public ReadChannel getReadChannel(long requestedPosition, int bytesToRead) throws IOException {
    updateStrategy(requestedPosition);
    ReadChannel channel = currentStrategy.getReadChannel(requestedPosition, bytesToRead);
    this.position = requestedPosition;

    return channel;
  }

  @Override
  public void close() throws IOException {
    currentStrategy.close();
    super.close();
  }

  @Override
  public void position(long newPosition) {
    super.position(newPosition);
    currentStrategy.position(newPosition);
    this.lastReadEndPosition = newPosition;
  }

  @Override
  public long getLimit() {
    return currentStrategy.getLimit();
  }

  @Override
  public Type getType() {
    return currentStrategy.getType();
  }

  ReadStrategy getDelegateStrategy() {
    return currentStrategy;
  }

  private void updateStrategy(long requestedPosition) throws IOException {
    if (shouldSwitchToRandom(requestedPosition)) {
      switchToRandom();
      return;
    }
    if (!isRandomMode || options.getFileAccessPattern() != FileAccessPattern.AUTO_RANDOM) {
      return;
    }
    if (!isSequentialRead(requestedPosition)) {
      sequentialReadCount = 0;
      return;
    }
    sequentialReadCount++;
    if (sequentialReadCount >= options.getAdaptiveReadSequentialReadThreshold()) {
      switchToSequential();
    }
  }

  private boolean shouldSwitchToRandom(long requestedPosition) {
    if (isRandomMode || options.getFileAccessPattern() != FileAccessPattern.AUTO_SEQUENTIAL) {
      return false;
    }

    return !isSequentialRead(requestedPosition);
  }

  private void switchToRandom() throws IOException {
    long startTime = System.nanoTime();
    try {
      isRandomMode = true;
      currentStrategy.close();
      currentStrategy = new RandomReadStrategy(storage, itemId, options, itemInfo, telemetry);
    } finally {
      recordSwitchDuration(startTime, Type.RANDOM);
    }
    telemetry.recordMetric(Metric.STRATEGY_SWITCH_TO_RANDOM_COUNT, 1, Collections.emptyMap());
  }

  private boolean isSequentialRead(long requestedPosition) {
    long seekDistance = requestedPosition - lastReadEndPosition;
    return seekDistance >= 0 && seekDistance <= options.getInplaceSeekLimit();
  }

  private void switchToSequential() throws IOException {
    long startTime = System.nanoTime();
    try {
      isRandomMode = false;
      sequentialReadCount = 0;
      currentStrategy.close();
      currentStrategy = new SequentialReadStrategy(storage, itemId, options, itemInfo, telemetry);
    } finally {
      recordSwitchDuration(startTime, Type.SEQUENTIAL);
    }
    telemetry.recordMetric(Metric.STRATEGY_SWITCH_TO_SEQUENTIAL_COUNT, 1, Collections.emptyMap());
  }

  private void recordSwitchDuration(long startTime, Type targetStrategy) {
    Metric metric =
        targetStrategy == Type.RANDOM
            ? Metric.STRATEGY_SWITCH_TO_RANDOM_DURATION
            : Metric.STRATEGY_SWITCH_TO_SEQUENTIAL_DURATION;
    telemetry.recordMetric(metric, System.nanoTime() - startTime, Collections.emptyMap());
  }
}
