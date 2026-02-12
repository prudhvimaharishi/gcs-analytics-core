package com.google.cloud.gcs.analyticscore.common.telemetry;

import com.google.auto.value.AutoValue;

/** Options for Logging Telemetry. */
@AutoValue
public abstract class LoggingTelemetryOptions {

  public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARNING,
    ERROR
  }

  public abstract LogLevel getLogLevel();

  public abstract boolean isEnabled();

  public static Builder builder() {
    return new AutoValue_LoggingTelemetryOptions.Builder()
        .setEnabled(false)
        .setLogLevel(LogLevel.INFO);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLogLevel(LogLevel logLevel);

    public abstract Builder setEnabled(boolean enabled);

    public abstract LoggingTelemetryOptions build();
  }
}
