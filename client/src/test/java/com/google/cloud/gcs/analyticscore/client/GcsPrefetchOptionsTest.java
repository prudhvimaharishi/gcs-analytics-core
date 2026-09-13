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

import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions.PrefetchMode;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsPrefetchOptionsTest {

  private static final String PREFIX = "gcs.";

  @Test
  void build_defaultValues_returnsDisabledMode() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.DISABLED);
  }

  @Test
  void createFromOptions_withEmptyOptions_returnsDefaults() {
    Map<String, String> map = ImmutableMap.of();

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options).isEqualTo(GcsPrefetchOptions.builder().build());
  }

  @Test
  void createFromOptions_mapWithPrefetchMode_parsesMode() {
    Map<String, String> map = prefetchModeOptions("PREDICTIVE_ROW_GROUP");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.PREDICTIVE_ROW_GROUP);
  }

  @Test
  void createFromOptions_lowerCaseMode_parsesMode() {
    Map<String, String> map = prefetchModeOptions("predictive_row_group");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.PREDICTIVE_ROW_GROUP);
  }

  @Test
  void createFromOptions_hyphenatedMode_parsesMode() {
    Map<String, String> map = prefetchModeOptions("predictive-row-group");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.PREDICTIVE_ROW_GROUP);
  }

  @Test
  void createFromOptions_unrecognisedMode_throwsIllegalArgumentException() {
    Map<String, String> map = prefetchModeOptions("not-a-mode");

    assertThrows(
        IllegalArgumentException.class, () -> GcsPrefetchOptions.createFromOptions(map, PREFIX));
  }

  @Test
  void createFromOptions_unrecognisedMode_exceptionMessageContainsOffendingKeyAndValue() {
    Map<String, String> map = prefetchModeOptions("not-a-mode");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsPrefetchOptions.createFromOptions(map, PREFIX));

    assertThat(exception)
        .hasMessageThat()
        .isEqualTo(
            String.format(
                "Invalid value 'not-a-mode' for key '%s'",
                PREFIX + GcsPrefetchOptions.PREFETCH_MODE_KEY));
  }

  @Test
  void createFromOptions_keysResolvedUnderNonEmptyPrefix_parsesMode() {
    Map<String, String> map =
        ImmutableMap.of("custom." + GcsPrefetchOptions.PREFETCH_MODE_KEY, "predictive-row-group");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, "custom.");

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.PREDICTIVE_ROW_GROUP);
  }

  @Test
  void createFromOptions_keyUnderDifferentPrefix_isIgnored() {
    Map<String, String> map =
        ImmutableMap.of("other." + GcsPrefetchOptions.PREFETCH_MODE_KEY, "predictive-row-group");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getPrefetchMode()).isEqualTo(PrefetchMode.DISABLED);
  }

  @Test
  void isEnabled_predictiveRowGroupMode_returnsTrue() {
    GcsPrefetchOptions options = optionsWithMode(PrefetchMode.PREDICTIVE_ROW_GROUP);

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void isEnabled_disabledMode_returnsFalse() {
    GcsPrefetchOptions options = optionsWithMode(PrefetchMode.DISABLED);

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void toBuilder_overriddenMode_roundTripsInstance() {
    GcsPrefetchOptions options = optionsWithMode(PrefetchMode.PREDICTIVE_ROW_GROUP);

    GcsPrefetchOptions roundTripped = options.toBuilder().build();

    assertThat(roundTripped).isEqualTo(options);
  }

  private static Map<String, String> prefetchModeOptions(String mode) {
    return ImmutableMap.of(PREFIX + GcsPrefetchOptions.PREFETCH_MODE_KEY, mode);
  }

  private static GcsPrefetchOptions optionsWithMode(PrefetchMode mode) {
    return GcsPrefetchOptions.builder().setPrefetchMode(mode).build();
  }
}
