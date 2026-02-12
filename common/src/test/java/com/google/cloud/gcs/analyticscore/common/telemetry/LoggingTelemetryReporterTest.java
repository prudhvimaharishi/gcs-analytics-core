package com.google.cloud.gcs.analyticscore.common.telemetry;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LoggingTelemetryReporterTest {

  @Test
  public void testLoggingOptionsDefaultValues() {
    LoggingTelemetryOptions options = LoggingTelemetryOptions.builder().build();

    assertThat(options.isEnabled()).isFalse();
    assertThat(options.getLogLevel()).isEqualTo(LoggingTelemetryOptions.LogLevel.INFO);
  }

  @Test
  public void testLoggingOptionsCustomValues() {
    LoggingTelemetryOptions options =
        LoggingTelemetryOptions.builder()
            .setEnabled(true)
            .setLogLevel(LoggingTelemetryOptions.LogLevel.DEBUG)
            .build();

    assertThat(options.isEnabled()).isTrue();
    assertThat(options.getLogLevel()).isEqualTo(LoggingTelemetryOptions.LogLevel.DEBUG);
  }
}
