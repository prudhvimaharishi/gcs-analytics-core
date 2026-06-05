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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.contrib.gcp.resource.GCPResourceProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A provider that programmatically builds an OpenTelemetry SDK explicitly configured with a
 * GoogleCloudMetricExporter.
 */
public class CloudMonitoringOpenTelemetryProvider implements OpenTelemetryProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudMonitoringOpenTelemetryProvider.class);

  private volatile OpenTelemetrySdk openTelemetrySdk;
  private final Duration exportInterval;
  private final Optional<String> projectId;

  public CloudMonitoringOpenTelemetryProvider(OpenTelemetryOptions openTelemetryOptions) {
    this.exportInterval = Duration.ofSeconds(openTelemetryOptions.getExportIntervalSeconds());
    this.projectId = openTelemetryOptions.getProjectId();
  }

  @Override
  public OpenTelemetry getOpenTelemetry() {
    if (openTelemetrySdk == null) {
      synchronized (this) {
        if (openTelemetrySdk == null) {
          openTelemetrySdk =
              OpenTelemetrySdk.builder().setMeterProvider(getCloudMonitoringExporter()).build();
        }
      }
    }
    return openTelemetrySdk;
  }

  @Override
  public void close() {
    if (openTelemetrySdk != null) {
      synchronized (this) {
        if (openTelemetrySdk != null) {
          try {
            CompletableResultCode flushResult = openTelemetrySdk.getSdkMeterProvider().forceFlush();
            if (!flushResult.join(10, TimeUnit.SECONDS).isSuccess()) {
              LOG.warn("OpenTelemetry SDK forceFlush timed out or failed before completion");
            }
          } catch (Exception e) {
            LOG.warn("Exception encountered during OpenTelemetry SDK forceFlush", e);
          } finally {
            openTelemetrySdk.close();
            openTelemetrySdk = null;
          }
        }
      }
    }
  }

  private SdkMeterProvider getCloudMonitoringExporter() {
    Resource gcpResource = Resource.empty();
    try {
      gcpResource = new GCPResourceProvider().createResource(null);
    } catch (Exception e) {
      LOG.warn("Failed to detect GCP platform attributes via GCPResourceProvider", e);
    }

    Resource resource =
        Resource.getDefault()
            .merge(gcpResource)
            .merge(
                Resource.builder()
                    .put(
                        AttributeKey.stringKey("service.instance.id"),
                        java.util.UUID.randomUUID().toString())
                    .build());

    SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder().setResource(resource);
    LOG.info("Detected Resource Attributes: {}", resource.getAttributes());

    MetricConfiguration.Builder configBuilder = MetricConfiguration.builder();
    projectId.ifPresent(configBuilder::setProjectId);

    MetricExporter cloudExporter =
        GoogleCloudMetricExporter.createWithConfiguration(configBuilder.build());

    PeriodicMetricReader metricReader =
        PeriodicMetricReader.builder(cloudExporter).setInterval(exportInterval).build();

    return meterProviderBuilder.registerMetricReader(metricReader).build();
  }
}
