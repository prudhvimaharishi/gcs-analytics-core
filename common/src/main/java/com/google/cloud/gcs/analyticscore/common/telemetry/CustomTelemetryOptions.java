package com.google.cloud.gcs.analyticscore.common.telemetry;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;

/** Options for Custom Telemetry. */
@AutoValue
public abstract class CustomTelemetryOptions {

  public abstract ImmutableList<OperationListener> getOperationListeners();

  public static Builder builder() {
    return new AutoValue_CustomTelemetryOptions.Builder().setOperationListeners(ImmutableList.of());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOperationListeners(List<OperationListener> operationListeners);

    public abstract CustomTelemetryOptions build();
  }
}
