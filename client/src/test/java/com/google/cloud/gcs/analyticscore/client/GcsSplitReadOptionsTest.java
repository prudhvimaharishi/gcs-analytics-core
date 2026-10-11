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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsSplitReadOptionsTest {

  private static final int MB = 1024 * 1024;

  @Test
  void createFromOptions_noProperties_isDisabled() {
    GcsSplitReadOptions options = GcsSplitReadOptions.createFromOptions(ImmutableMap.of(), "gcs.");

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void createFromOptions_noProperties_usesEightMibThreshold() {
    GcsSplitReadOptions options = GcsSplitReadOptions.createFromOptions(ImmutableMap.of(), "gcs.");

    assertThat(options.getThresholdBytes()).isEqualTo(8 * MB);
  }

  @Test
  void createFromOptions_enabledProperty_isEnabled() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("gcs.analytics-core.read.split.enabled", "true");

    GcsSplitReadOptions options = GcsSplitReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void createFromOptions_thresholdProperty_usesThreshold() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("gcs.analytics-core.read.split.threshold-bytes", "16777216");

    GcsSplitReadOptions options = GcsSplitReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getThresholdBytes()).isEqualTo(16 * MB);
  }

  @Test
  void createFromOptions_thresholdBelowOneMib_throwsIllegalArgumentException() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("gcs.analytics-core.read.split.threshold-bytes", "8192");

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsSplitReadOptions.createFromOptions(properties, "gcs."));

    assertThat(e)
        .hasMessageThat()
        .isEqualTo(
            "analytics-core.read.split.threshold-bytes must be at least 1048576 bytes but was"
                + " 8192");
  }
}
