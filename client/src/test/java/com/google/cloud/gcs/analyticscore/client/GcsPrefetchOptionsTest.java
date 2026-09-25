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
import org.junit.jupiter.api.Test;

class GcsPrefetchOptionsTest {

  @Test
  void build_defaultValues_returnsDisabledMode() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

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
            .build();

    assertThat(options.isEnabled()).isFalse();
  }

  private static GcsPrefetchOptions optionsWithMode(PrefetchMode mode) {
    return GcsPrefetchOptions.builder().setPrefetchMode(mode).build();
  }
}
