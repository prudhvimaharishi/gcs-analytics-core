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
package com.google.cloud.gcs.analyticscore.common;

public class GcsAnalyticsCoreTelemetryConstants {
  public enum Attribute {
    CLASS_NAME,
    READ_LENGTH,
    READ_OFFSET;
  }

  public enum Metric implements com.google.cloud.gcs.analyticscore.common.telemetry.Metric {
    SEEK_DISTANCE("gcs.analytics-core.client.seek.size", MetricType.COUNTER),
    SEEK_DURATION("gcs.analytics-core.client.seek.duration", MetricType.DURATION),
    READ_BYTES("gcs.analytics-core.client.read.size", MetricType.COUNTER),
    READ_DURATION("gcs.analytics-core.client.read.duration", MetricType.DURATION),
    OPEN_DURATION("gcs.analytics-core.client.open.duration", MetricType.DURATION),
    READ_CACHE_HIT("gcs.analytics-core.client.read.cache.hits", MetricType.COUNTER),
    READ_CACHE_MISS("gcs.analytics-core.client.read.cache.misses", MetricType.COUNTER),
    CLOSE_DURATION("gcs.analytics-core.client.close.duration", MetricType.DURATION),
    GCS_CLIENT_CREATE_DURATION("gcs.analytics-core.client.create.duration", MetricType.DURATION),
    HARD_SEEK_COUNT("gcs.analytics-core.client.seek.hard.count", MetricType.COUNTER),
    HARD_SEEK_BYTES("gcs.analytics-core.client.seek.hard.bytes", MetricType.COUNTER),
    INPLACE_SEEK_COUNT("gcs.analytics-core.client.seek.inplace.count", MetricType.COUNTER),
    INPLACE_SEEK_BYTES("gcs.analytics-core.client.seek.inplace.bytes", MetricType.COUNTER),
    CHANNEL_OPEN_COUNT("gcs.analytics-core.client.channel.open.count", MetricType.COUNTER),
    STRATEGY_SWITCH_TO_RANDOM_COUNT(
        "gcs.analytics-core.client.strategy.switch.random.count", MetricType.COUNTER),
    STRATEGY_SWITCH_TO_SEQUENTIAL_COUNT(
        "gcs.analytics-core.client.strategy.switch.sequential.count", MetricType.COUNTER);

    private final String name;
    private final MetricType type;

    Metric(String name, MetricType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public MetricType getType() {
      return type;
    }
  }

  public enum Operation {
    SEEK,
    READ,
    CLOSE,
    READ_FULLY,
    READ_TAIL,
    OPEN,
    VECTORED_READ,
    GCS_CLIENT_CREATE;
  }
}
