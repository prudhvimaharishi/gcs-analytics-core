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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.contrib.gcp.resource.GCPResourceProvider;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

/** Manages the lifecycle of OpenTelemetry metrics for Google Cloud Monitoring. */
public class GoogleCloudMonitoringMetricsManager {

    private static SdkMeterProvider METER_PROVIDER;

    private static Meter METER;
    private static final Random RANDOM = new Random();

    private static MetricConfiguration generateMetricExporterConfig(boolean useDefaultConfig)
            throws IOException {
        if (useDefaultConfig) {
            System.out.println("Using default exporter configuration");
            return MetricConfiguration.builder().build();
        }

        System.out.println("Using custom configuration");
        // Configuring exporter through MetricServiceSettings
        Credentials credentials = GoogleCredentials.getApplicationDefault();
        MetricServiceSettings.Builder metricServiceSettingsBuilder = MetricServiceSettings.newBuilder();
        metricServiceSettingsBuilder
                .setCredentialsProvider(
                        FixedCredentialsProvider.create(credentials))
                .setTransportChannelProvider(
                        FixedTransportChannelProvider.create(
                                GrpcTransportChannel.create(
                                        ManagedChannelBuilder.forTarget(
                                                        MetricConfiguration.DEFAULT_METRIC_SERVICE_ENDPOINT)
                                                // default 8 KiB
                                                .maxInboundMetadataSize(16 * 1000)
                                                .build())))
                .createMetricDescriptorSettings()
                .setSimpleTimeoutNoRetries(
                        org.threeten.bp.Duration.ofMillis(MetricConfiguration.DEFAULT_DEADLINE.toMillis()))
                .build();

        // Any properties not set would be retrieved from the default configuration of the exporter.
        return MetricConfiguration.builder()
                .setMetricServiceSettings(metricServiceSettingsBuilder.build())
                .setInstrumentationLibraryLabelsEnabled(false)
                .build();
    }

    private static void setupMetricExporter(MetricConfiguration metricConfiguration) {
        GCPResourceProvider resourceProvider = new GCPResourceProvider();
        MetricExporter metricExporter =
                GoogleCloudMetricExporter.createWithConfiguration(metricConfiguration);
        MetricExporter metricDebugExporter = LoggingMetricExporter.create();
        METER_PROVIDER =
                SdkMeterProvider.builder()
                        .setResource(Resource.create(resourceProvider.getAttributes()))
                        .registerMetricReader(
                                PeriodicMetricReader.builder(metricExporter)
                                        .setInterval(Duration.ofSeconds(2))
                                        .build())
                        .registerMetricReader(
                                PeriodicMetricReader.builder(metricDebugExporter)
                                        .setInterval(Duration.ofSeconds(2))
                                        .build())
                        .build();

        METER =
                METER_PROVIDER
                        .meterBuilder("instrumentation-library-name")
                        .setInstrumentationVersion("semver:1.0.0")
                        .build();
    }

    public static Meter getMeter() throws IOException {
        setupMetricExporter(generateMetricExporterConfig(false));
        return METER;
    }
}
