/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsCacheOptionsTest {

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  @Test
  void build_defaultValues_succeeds() {
    GcsCacheOptions options = GcsCacheOptions.builder().build();

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(1024 * MB);
    assertThat(options.isSmallObjectCacheEnabled()).isFalse();
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(1024 * MB);
    assertThat(options.isWorkerCacheEnabled()).isFalse();
    assertThat(options.getWorkerCacheDirectory()).contains("gcs-analytics-cache");
    assertThat(options.getWorkerCacheMaxSizeBytes()).isEqualTo(10240 * MB);
  }

  @Test
  void build_disabledCacheNonPositiveSizeBytes_succeeds() {
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(false)
            .setFooterCacheMaxSizeBytes(0)
            .setSmallObjectCacheEnabled(false)
            .setSmallObjectCacheMaxSizeBytes(0)
            .setWorkerCacheEnabled(false)
            .setWorkerCacheMaxSizeBytes(0)
            .build();

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(0);
    assertThat(options.isSmallObjectCacheEnabled()).isFalse();
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(0);
    assertThat(options.isWorkerCacheEnabled()).isFalse();
    assertThat(options.getWorkerCacheMaxSizeBytes()).isEqualTo(0);
  }

  @Test
  void build_enabledCacheZeroSizeBytes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledWorkerCacheZeroSizeBytes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setWorkerCacheEnabled(true).setWorkerCacheMaxSizeBytes(0);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledWorkerCacheNullDirectory_throwsException() {
    GcsCacheOptions.Builder builder = GcsCacheOptions.builder().setWorkerCacheEnabled(true);

    assertThrows(NullPointerException.class, () -> builder.setWorkerCacheDirectory(null));
  }

  @Test
  void build_enabledWorkerCacheEmptyDirectory_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setWorkerCacheEnabled(true).setWorkerCacheDirectory("   ");

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void build_enabledCacheNegativeSizeBytes_throwsException() {
    GcsCacheOptions.Builder builder =
        GcsCacheOptions.builder().setFooterCacheEnabled(true).setFooterCacheMaxSizeBytes(-1);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void createFromOptions_withAllOptions_succeeds() {
    boolean footerCacheEnabled = false;
    long footerCacheMaxSizeBytes = 50 * MB;
    boolean smallObjectCacheEnabled = true;
    long smallObjectCacheMaxSizeBytes = 100 * MB;
    boolean workerCacheEnabled = true;
    String workerCacheDirectory = "/tmp/custom-worker-cache";
    long workerCacheMaxSizeBytes = 5000 * MB;

    Map<String, String> map = new HashMap<>();
    map.put("gcs." + GcsCacheOptions.FOOTER_CACHE_ENABLED_KEY, String.valueOf(footerCacheEnabled));
    map.put(
        "gcs." + GcsCacheOptions.FOOTER_CACHE_MAX_SIZE_BYTES_KEY,
        String.valueOf(footerCacheMaxSizeBytes));
    map.put(
        "gcs." + GcsCacheOptions.SMALL_FILE_CACHE_ENABLED_KEY,
        String.valueOf(smallObjectCacheEnabled));
    map.put(
        "gcs." + GcsCacheOptions.SMALL_FILE_CACHE_MAX_SIZE_BYTES_KEY,
        String.valueOf(smallObjectCacheMaxSizeBytes));
    map.put("gcs." + GcsCacheOptions.WORKER_CACHE_ENABLED_KEY, String.valueOf(workerCacheEnabled));
    map.put("gcs." + GcsCacheOptions.WORKER_CACHE_DIRECTORY_KEY, workerCacheDirectory);
    map.put(
        "gcs." + GcsCacheOptions.WORKER_CACHE_MAX_SIZE_BYTES_KEY,
        String.valueOf(workerCacheMaxSizeBytes));

    GcsCacheOptions options = GcsCacheOptions.createFromOptions(map, "gcs.");

    assertThat(options.isFooterCacheEnabled()).isEqualTo(footerCacheEnabled);
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(footerCacheMaxSizeBytes);
    assertThat(options.isSmallObjectCacheEnabled()).isEqualTo(smallObjectCacheEnabled);
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(smallObjectCacheMaxSizeBytes);
    assertThat(options.isWorkerCacheEnabled()).isEqualTo(workerCacheEnabled);
    assertThat(options.getWorkerCacheDirectory()).isEqualTo(workerCacheDirectory);
    assertThat(options.getWorkerCacheMaxSizeBytes()).isEqualTo(workerCacheMaxSizeBytes);
  }

  @Test
  void createFromOptions_withEmptyOptions_returnsDefaults() {
    Map<String, String> map = new HashMap<>();

    GcsCacheOptions options = GcsCacheOptions.createFromOptions(map, "gcs.");

    assertThat(options.isFooterCacheEnabled()).isFalse();
    assertThat(options.getFooterCacheMaxSizeBytes()).isEqualTo(1024 * MB);
    assertThat(options.isSmallObjectCacheEnabled()).isFalse();
    assertThat(options.getSmallObjectCacheMaxSizeBytes()).isEqualTo(1024 * MB);
    assertThat(options.isWorkerCacheEnabled()).isFalse();
    assertThat(options.getWorkerCacheDirectory()).contains("gcs-analytics-cache");
    assertThat(options.getWorkerCacheMaxSizeBytes()).isEqualTo(10240 * MB);
  }

  @Test
  void createFromOptions_malformedInteger_throwsNumberFormatException() {
    Map<String, String> map = new HashMap<>();
    map.put("gcs." + GcsCacheOptions.FOOTER_CACHE_MAX_SIZE_BYTES_KEY, "not-a-number");

    assertThrows(NumberFormatException.class, () -> GcsCacheOptions.createFromOptions(map, "gcs."));
  }
}
