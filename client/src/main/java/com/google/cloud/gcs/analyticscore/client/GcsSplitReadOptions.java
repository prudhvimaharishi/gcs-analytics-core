/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import java.util.Map;

/** Configuration options for splitting large reads into parallel part requests. */
@AutoValue
public abstract class GcsSplitReadOptions {

  private static final String ENABLED_KEY = "analytics-core.read.split.enabled";
  private static final String THRESHOLD_KEY = "analytics-core.read.split.threshold-bytes";

  private static final int MB = 1024 * 1024;
  private static final int MIN_THRESHOLD_BYTES = MB;

  // Whether reads larger than the threshold are split into parallel part requests.
  abstract boolean isEnabled();

  // Reads larger than this are split into parts of at most this many bytes.
  abstract int getThresholdBytes();

  abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_GcsSplitReadOptions.Builder().setEnabled(false).setThresholdBytes(8 * MB);
  }

  public static GcsSplitReadOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + ENABLED_KEY)) {
      optionsBuilder.setEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + THRESHOLD_KEY)) {
      optionsBuilder.setThresholdBytes(
          ConfigurationUtil.safeParseInteger(analyticsCoreOptions, prefix + THRESHOLD_KEY));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsSplitReadOptions}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setEnabled(boolean enabled);

    abstract Builder setThresholdBytes(int thresholdBytes);

    abstract GcsSplitReadOptions autoBuild();

    GcsSplitReadOptions build() {
      GcsSplitReadOptions options = autoBuild();
      checkArgument(
          options.getThresholdBytes() >= MIN_THRESHOLD_BYTES,
          "%s must be at least %s bytes but was %s",
          THRESHOLD_KEY,
          MIN_THRESHOLD_BYTES,
          options.getThresholdBytes());
      return options;
    }
  }
}
