package com.google.cloud.gcs.analyticscore.client;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.contrib.gcp.resource.GCPResourceProvider;
import io.opentelemetry.exporter.logging.LoggingMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class GcsAnalyticsCoreInstrumentation
{
  private static final Logger LOG = LoggerFactory.getLogger(GcsAnalyticsCoreInstrumentation.class);

  private final GcsAnalyticsCoreMetrics analyticsCoreMetrics;
  private OpenTelemetrySdk openTelemetrySdk;

  private GcsAnalyticsCoreInstrumentation() {
    initializeSdk();
    Meter meter = GlobalOpenTelemetry.getMeter("com.google.cloud.gcs.analyticscore");
    this.analyticsCoreMetrics = new GcsAnalyticsCoreMetrics(meter);
  }

  public static GcsAnalyticsCoreInstrumentation get() {
    return GcsAnalyticsCoreInstrumentationHolder.INSTANCE;
  }

  public GcsAnalyticsCoreMetrics metrics() {
    return analyticsCoreMetrics;
  }

  private void initializeSdk() {
    try {
      LOG.info("initialziing sdk");
      Resource gcpResource = new GCPResourceProvider().createResource(null);

      String executorId = System.getProperty("spark.executor.id");
      if (executorId == null) executorId = System.getenv("SPARK_EXECUTOR_ID");
      if (executorId == null) executorId = "unknown-" + UUID.randomUUID().toString().substring(0, 8);

      String appId = System.getProperty("spark.app.id", System.getenv("SPARK_APPLICATION_ID"));
      if (appId == null) appId = "unknown-app";

      Resource finalResource = Resource.getDefault()
          .merge(gcpResource)
          .merge(Resource.create(Attributes.builder()
              .put("service.name", "gcs-analytics-metrics-library")
              .put("service.instance.id", appId + "-" + executorId)
              .put("service.executor.id", UUID.randomUUID().toString())
              .build()));

      MetricExporter exporter =
          LoggingMetricExporter.create();

      openTelemetrySdk = OpenTelemetrySdk.builder()
          .setMeterProvider(SdkMeterProvider.builder()
              .setResource(finalResource)
              .registerMetricReader(PeriodicMetricReader.builder(exporter)
                  .setInterval(Duration.ofSeconds(60))
                  .build())
              .build())
          .buildAndRegisterGlobal();

      // Adding a JVM shutdown hook for flushing remaining metrics.
      Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

    } catch (Exception e) {
      System.err.println("GCS Metrics: Failed to initialize SDK. Continuing without metrics.");
      e.printStackTrace();
    }
  }

  private static class GcsAnalyticsCoreInstrumentationHolder {
    private static final GcsAnalyticsCoreInstrumentation INSTANCE = new GcsAnalyticsCoreInstrumentation();
  }

  private void shutdown() {
    if (this.openTelemetrySdk != null) {
      LOG.info("GCS Analytics Core Metrics: Flushing and shutting down...");
      // Give it 5 seconds to flush remaining metrics.
      this.openTelemetrySdk.getSdkMeterProvider().forceFlush().join(1, TimeUnit.SECONDS);
      this.openTelemetrySdk.shutdown().join(1, TimeUnit.SECONDS);
      openTelemetrySdk = null;
    }
  }

  public void flush(){
    if(openTelemetrySdk!=null) {
      this.openTelemetrySdk.getSdkMeterProvider().forceFlush().join(1, TimeUnit.SECONDS);
    }
  }

}
