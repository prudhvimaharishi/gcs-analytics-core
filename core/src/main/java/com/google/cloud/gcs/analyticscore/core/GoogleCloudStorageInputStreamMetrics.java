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
package com.google.cloud.gcs.analyticscore.core;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;

import java.io.IOException;

/** Metrics for GoogleCloudStorageInputStream. */
public final class GoogleCloudStorageInputStreamMetrics {

  private static final String INSTRUMENTATION_NAME =
      GoogleCloudStorageInputStreamMetrics.class.getName();
  private static final Meter METER;

  static {
      METER = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_NAME);
  }

  private static final LongCounter SEEK_COUNT =
    METER
        .counterBuilder("gcs.input_stream.seek.count")
        .setDescription("Number of seek operations.")
        .setUnit("1")
        .build();

  private static final LongCounter READ_COUNT =
      METER
          .counterBuilder("gcs.input_stream.read.count")
          .setDescription("Number of read operations.")
          .setUnit("1")
          .build();

  private static final LongCounter BYTES_READ =
           METER
              .counterBuilder("gcs.input_stream.bytes.read")
              .setDescription("Number of bytes read.")
              .setUnit("1")
              .build();

  private static final LongCounter READ_FULLY_COUNT =
      METER
          .counterBuilder("gcs.input_stream.read_fully.count")
          .setDescription("Number of readFully operations.")
          .setUnit("1")
          .build();

  private static final LongCounter READ_TAIL_COUNT =
      METER
          .counterBuilder("gcs.input_stream.read_tail.count")
          .setDescription("Number of readTail operations.")
          .setUnit("1")
          .build();

  private static final LongCounter READ_VECTORED_COUNT =
      METER
          .counterBuilder("gcs.input_stream.read_vectored.count")
          .setDescription("Number of readVectored operations.")
          .setUnit("1")
          .build();

  private static final LongCounter CLOSE_COUNT =
      METER
          .counterBuilder("gcs.input_stream.close.count")
          .setDescription("Number of close operations.")
          .setUnit("1")
          .build();

  public static void recordSeek() {
    SEEK_COUNT.add(1);
  }

  public static void recordRead(int bytesRead) {
    READ_COUNT.add(1);
    if (bytesRead > 0) {
      BYTES_READ.add(bytesRead);
    }
  }

  public static void recordReadFully(int bytesRead) {
    READ_FULLY_COUNT.add(1);
    if (bytesRead > 0) {
        BYTES_READ.add(bytesRead);
    }
  }

  public static void recordReadTail(int bytesRead) {
    READ_TAIL_COUNT.add(1);
    if (bytesRead > 0) {
        BYTES_READ.add(bytesRead);
    }
  }

  public static void recordReadVectored() {
    READ_VECTORED_COUNT.add(1);
  }

  public static void recordClose() {
    CLOSE_COUNT.add(1);
  }
}
