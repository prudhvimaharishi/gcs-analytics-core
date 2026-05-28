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

package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricKey;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.core.ParquetMetadataCache.ParquetObjectMetadata;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetMetadataCacheTest {

  @TempDir Path tempDir;

  private Storage mockStorage;
  private Telemetry telemetry;
  private Map<MetricKey, Long> recordedMetrics;

  @BeforeEach
  void setUp() {
    mockStorage = mock(Storage.class);
    recordedMetrics = new ConcurrentHashMap<>();
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
            recordedMetrics.putAll(metrics);
          }
        };
    telemetry = new Telemetry(ImmutableList.of(listener));
  }

  @AfterEach
  void tearDown() {
    ParquetMetadataCache.setInstance(null);
  }

  @Test
  void getMetadata_downloadsIndex_recordsCountAndBytesMetrics() throws IOException {
    String baseGcsPath = "gs://test-bucket/path/to/";
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.getName()).thenReturn("path/to/parquet_metadata_index.db");
    when(mockBlob.exists()).thenReturn(true);
    when(mockBlob.getSize()).thenReturn(54321L);
    doAnswer(
            invocation -> {
              Path path = invocation.getArgument(0);
              Files.createFile(path);
              return null;
            })
        .when(mockBlob)
        .downloadTo(any(Path.class));
    @SuppressWarnings("unchecked")
    Page<Blob> mockPage = mock(Page.class);
    when(mockPage.iterateAll()).thenReturn(ImmutableList.of(mockBlob));
    when(mockStorage.list(eq("test-bucket"), any(Storage.BlobListOption.class)))
        .thenReturn(mockPage);
    when(mockStorage.get(eq("test-bucket"), eq("path/to/parquet_metadata_index.db")))
        .thenReturn(mockBlob);
    ParquetMetadataCache cache =
        new ParquetMetadataCache(
            baseGcsPath, tempDir.getFileName().toString(), telemetry, mockStorage);
    ParquetMetadataCache.setInstance(cache);

    Optional<ParquetObjectMetadata> metadata =
        cache.getMetadata("gs://test-bucket/path/to/file.parquet");

    assertThat(metadata.isPresent()).isFalse();
    MetricKey listCountKey = MetricKey.builder().setMetric(Metric.FOOTER_INDEX_LIST_COUNT).build();
    MetricKey countKey = MetricKey.builder().setMetric(Metric.FOOTER_INDEX_DOWNLOAD_COUNT).build();
    MetricKey bytesKey = MetricKey.builder().setMetric(Metric.FOOTER_INDEX_DOWNLOAD_BYTES).build();
    assertThat(recordedMetrics.get(listCountKey)).isEqualTo(1L);
    assertThat(recordedMetrics.get(countKey)).isEqualTo(1L);
    assertThat(recordedMetrics.get(bytesKey)).isEqualTo(54321L);
  }
}
