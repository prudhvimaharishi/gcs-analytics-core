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

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.trace.TraceConfiguration;
import com.google.cloud.opentelemetry.trace.TraceExporter;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.IOException;
import java.time.Duration;

/** Manages the lifecycle of OpenTelemetry metrics for Google Cloud Monitoring. */
public class GoogleCloudMonitoringMetricsManager {

  private static SdkMeterProvider meterProvider;
  private static SdkTracerProvider tracerProvider;
  private static TraceExporter traceExporter;

  /**
   * Initializes the OpenTelemetry SDK and configures the exporter for Google Cloud Monitoring.
   *
   * @throws IOException if the metric exporter cannot be created.
   */
  public static void initialize() throws IOException {
    if (meterProvider != null) {
      return;
    }
    MetricExporter metricExporter =
            GoogleCloudMetricExporter.createWithDefaultConfiguration();

    meterProvider =
        SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(60))
                    .build())
            .build();

      TraceExporter.createWithConfiguration(
              TraceConfiguration.builder().setProjectId("pbeerelly-test-sandbox-629413").build()
      );
    tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
            .build();

    OpenTelemetrySdk.builder()
        .setMeterProvider(meterProvider)
        .setTracerProvider(tracerProvider)
        .buildAndRegisterGlobal();
  }

  /** Shuts down the OpenTelemetry SDK and releases resources. */
  public static void shutdown() {
    if (meterProvider != null) {
      meterProvider.shutdown();
      meterProvider = null;
    }
    if (tracerProvider != null) {
      tracerProvider.shutdown();
      tracerProvider = null;
    }
    if (traceExporter != null) {
      traceExporter.shutdown();
      traceExporter = null;
    }
    GlobalOpenTelemetry.resetForTest();
  }
}
