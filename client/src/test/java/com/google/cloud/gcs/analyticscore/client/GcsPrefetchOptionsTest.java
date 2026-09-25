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

import org.junit.jupiter.api.Test;

class GcsPrefetchOptionsTest {

  @Test
  void build_defaultValues_returnsDisabled() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().build();

    assertThat(options.isEnabled()).isFalse();
  }

  @Test
  void isEnabled_setEnabledTrue_returnsTrue() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().setEnabled(true).build();

    assertThat(options.isEnabled()).isTrue();
  }

  @Test
  void toBuilder_enabledOptions_roundTripsInstance() {
    GcsPrefetchOptions options = GcsPrefetchOptions.builder().setEnabled(true).build();

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
        GcsPrefetchOptions.builder().setEnabled(true).setBufferCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledWithNegativeBufferCacheTtlSeconds_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(true).setBufferCacheTtlSeconds(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_disabledWithZeroBufferCacheMaxSizeBytes_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(false).setBufferCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_disabledWithZeroBufferCacheTtlSeconds_throwsIllegalArgumentException() {
    GcsPrefetchOptions.Builder builder =
        GcsPrefetchOptions.builder().setEnabled(false).setBufferCacheTtlSeconds(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }
}
