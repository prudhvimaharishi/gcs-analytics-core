package com.google.cloud.gcs.analyticscore.client;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class GcsAnalyticsCoreMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(GcsAnalyticsCoreMetrics.class);

  private final LongCounter bytesReadCount;
  private final AtomicLong bytesRead = new AtomicLong(0);

  public GcsAnalyticsCoreMetrics(Meter meter) {
    this.bytesReadCount = meter.counterBuilder("gcs.analytics-core.bytes.read")
        .setDescription("Total bytes read using GCS Analytics Core Input Stream")
        .setUnit("By")
        .build();
  }

  public void recordBytesRead(long bytesRead) {
    LOG.info("recording bytes read");
    this.bytesRead.addAndGet(bytesRead);
    this.bytesReadCount.add(bytesRead);
  }
}
