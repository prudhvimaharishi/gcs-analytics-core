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

  @Test
  void build_defaultValues_returnsDefaultBufferCacheMaxSizeBytes() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getBufferCacheMaxSizeBytes()).isEqualTo(2L * 1024 * 1024 * 1024);
  }

  @Test
  void build_defaultValues_returnsDefaultBufferCacheTtlSeconds() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getBufferCacheTtlSeconds()).isEqualTo(60);
  }

  @Test
  void createFromOptions_mapWithBufferCacheMaxSizeBytes_parsesMaxSizeBytes() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.BUFFER_CACHE_MAX_SIZE_BYTES_KEY, "1024");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getBufferCacheMaxSizeBytes()).isEqualTo(1024);
  }

  @Test
  void createFromOptions_mapWithBufferCacheTtlSeconds_parsesTtlSeconds() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.BUFFER_CACHE_TTL_SECONDS_KEY, "30");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getBufferCacheTtlSeconds()).isEqualTo(30);
  }

  @Test
  void build_enabledWithZeroBufferCacheMaxSizeBytes_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP)
            .setBufferCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledWithNegativeBufferCacheTtlSeconds_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP)
            .setBufferCacheTtlSeconds(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_disabledWithNonPositiveValues_succeeds() {
    GcsPrefetchOptions options =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.DISABLED)
            .setBufferCacheMaxSizeBytes(0)
            .setBufferCacheTtlSeconds(0)
            .setHistoryMaxColumns(0)
            .build();

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void build_defaultValues_returnsDefaultBlockSizeBytes() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getBlockSizeBytes()).isEqualTo(4 * 1024 * 1024);
  }

  @Test
  void createFromOptions_mapWithBlockSizeBytes_parsesBlockSizeBytes() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.BLOCK_SIZE_BYTES_KEY, "65536");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getBlockSizeBytes()).isEqualTo(65536);
  }

  @Test
  void build_enabledWithZeroBlockSizeBytes_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP)
            .setBlockSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_defaultValues_returnsDefaultHistoryMaxColumns() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.getHistoryMaxColumns()).isEqualTo(256);
  }

  @Test
  void createFromOptions_mapWithHistoryMaxColumns_parsesHistoryMaxColumns() {
    Map<String, String> map =
        ImmutableMap.of(PREFIX + GcsPrefetchOptions.HISTORY_MAX_COLUMNS_KEY, "42");

    GcsPrefetchOptions options = GcsPrefetchOptions.createFromOptions(map, PREFIX);

    assertThat(options.getHistoryMaxColumns()).isEqualTo(42);
  }

  @Test
  void build_enabledWithZeroHistoryMaxColumns_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder()
            .setPrefetchMode(PrefetchMode.PREDICTIVE_ROW_GROUP)
            .setHistoryMaxColumns(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  private static Map<String, String> prefetchModeOptions(String mode) {
    return ImmutableMap.of(PREFIX + GcsPrefetchOptions.PREFETCH_MODE_KEY, mode);
  }

  private static GcsPrefetchOptions optionsWithMode(PrefetchMode mode) {
    return GcsPrefetchOptions.builder().setPrefetchMode(mode).build();
  }
}
