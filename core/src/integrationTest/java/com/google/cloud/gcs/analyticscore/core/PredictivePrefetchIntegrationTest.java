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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@EnabledIfSystemProperty(named = "gcs.integration.test.bucket", matches = ".+")
@EnabledIfSystemProperty(named = "gcs.integration.test.project-id", matches = ".+")
class PredictivePrefetchIntegrationTest {
  private static final String PREFETCH_MODE_KEY = "gcs.analytics-core.prefetch.mode";
  private static final String DISABLED_MODE = "disabled";
  private static final String PREDICTIVE_ROW_GROUP_MODE = "predictive-row-group";

  private static final String NON_PARQUET_OBJECT_NAME = "predictive_prefetch_payload.txt";
  private static final String NON_PARQUET_CONTENT =
      "predictive prefetch integration test payload\n".repeat(512);

  private static final String PROJECTED_SCHEMA =
      "message requested_schema {\n"
          + "required binary c_customer_id (STRING);\n"
          + "optional binary c_first_name (STRING);\n"
          + "optional binary c_email_address (STRING);\n"
          + "}";

  private static final int READ_BUFFER_BYTES = 65536;
  private static final int SAMPLED_RANGE_BYTES = 65536;
  private static final int SAMPLED_RANGE_COUNT = 8;

  @BeforeAll
  public static void uploadTestObjectsToGcs() throws IOException {
    IntegrationTestHelper.uploadSampleParquetFilesIfNotExists();
    IntegrationTestHelper.uploadFileToGcs(
        new ByteArrayInputStream(NON_PARQUET_CONTENT.getBytes(UTF_8)), NON_PARQUET_OBJECT_NAME);
  }

  @ParameterizedTest
  @MethodSource("prefetchModeAndFileArguments")
  void readWholeObject_prefetchEnabled_returnsSameBytesAsPrefetchDisabled(
      String prefetchMode, String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    byte[] expectedBytes = readWholeObject(uri, createOptions(DISABLED_MODE));

    byte[] actualBytes = readWholeObject(uri, createOptions(prefetchMode));

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  @ParameterizedTest
  @MethodSource("prefetchModeAndLargeFileArguments")
  void readSampledRanges_prefetchEnabled_returnsSameBytesAsPrefetchDisabled(
      String prefetchMode, String fileName) throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(fileName);
    byte[] expectedBytes = readSampledRanges(uri, createOptions(DISABLED_MODE));

    byte[] actualBytes = readSampledRanges(uri, createOptions(prefetchMode));

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  @ParameterizedTest
  @MethodSource("prefetchModeAndVectoredReadArguments")
  void readProjectedRecords_prefetchEnabled_returnsSameRecordCountAsPrefetchDisabled(
      String prefetchMode, boolean readVectoredEnabled) {
    URI uri =
        IntegrationTestHelper.getGcsObjectUriForFile(
            IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE);
    long expectedRecordCount =
        ParquetHelper.readParquetObjectRecords(
            uri, PROJECTED_SCHEMA, readVectoredEnabled, createOptions(DISABLED_MODE));

    long actualRecordCount =
        ParquetHelper.readParquetObjectRecords(
            uri, PROJECTED_SCHEMA, readVectoredEnabled, createOptions(prefetchMode));

    assertThat(actualRecordCount).isEqualTo(expectedRecordCount);
  }

  @ParameterizedTest
  @ValueSource(strings = {PREDICTIVE_ROW_GROUP_MODE})
  void readNonParquetObject_prefetchEnabled_returnsUploadedContent(String prefetchMode)
      throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(NON_PARQUET_OBJECT_NAME);

    byte[] actualBytes = readWholeObject(uri, createOptions(prefetchMode));

    assertThat(new String(actualBytes, UTF_8)).isEqualTo(NON_PARQUET_CONTENT);
  }

  @ParameterizedTest
  @ValueSource(strings = {PREDICTIVE_ROW_GROUP_MODE})
  void readNonParquetObject_prefetchEnabled_returnsSameBytesAsPrefetchDisabled(String prefetchMode)
      throws IOException {
    URI uri = IntegrationTestHelper.getGcsObjectUriForFile(NON_PARQUET_OBJECT_NAME);
    byte[] expectedBytes = readWholeObject(uri, createOptions(DISABLED_MODE));

    byte[] actualBytes = readWholeObject(uri, createOptions(prefetchMode));

    assertThat(actualBytes).isEqualTo(expectedBytes);
  }

  static Stream<Arguments> prefetchModeAndFileArguments() {
    return Stream.of(
        arguments(PREDICTIVE_ROW_GROUP_MODE, IntegrationTestHelper.TPCDS_CUSTOMER_SMALL_FILE),
        arguments(PREDICTIVE_ROW_GROUP_MODE, IntegrationTestHelper.TPCDS_CUSTOMER_MEDIUM_FILE));
  }

  static Stream<Arguments> prefetchModeAndLargeFileArguments() {
    return Stream.of(
        arguments(PREDICTIVE_ROW_GROUP_MODE, IntegrationTestHelper.TPCDS_CUSTOMER_LARGE_FILE));
  }

  static Stream<Arguments> prefetchModeAndVectoredReadArguments() {
    return Stream.of(
        arguments(PREDICTIVE_ROW_GROUP_MODE, true),
        arguments(PREDICTIVE_ROW_GROUP_MODE, false));
  }

  private static GcsFileSystemOptions createOptions(String prefetchMode) {
    return GcsFileSystemOptions.createFromOptions(Map.of(PREFETCH_MODE_KEY, prefetchMode), "gcs.");
  }

  private static byte[] readWholeObject(URI uri, GcsFileSystemOptions gcsFileSystemOptions)
      throws IOException {
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
        GoogleCloudStorageInputStream inputStream =
            GoogleCloudStorageInputStream.create(gcsFileSystem, uri)) {
      ByteArrayOutputStream collectedBytes = new ByteArrayOutputStream();
      byte[] buffer = new byte[READ_BUFFER_BYTES];
      int bytesRead = inputStream.read(buffer, 0, buffer.length);
      while (bytesRead != -1) {
        collectedBytes.write(buffer, 0, bytesRead);
        bytesRead = inputStream.read(buffer, 0, buffer.length);
      }
      return collectedBytes.toByteArray();
    }
  }

  private static byte[] readSampledRanges(URI uri, GcsFileSystemOptions gcsFileSystemOptions)
      throws IOException {
    try (GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(gcsFileSystemOptions);
        GoogleCloudStorageInputStream inputStream =
            GoogleCloudStorageInputStream.create(gcsFileSystem, uri)) {
      long fileSize = gcsFileSystem.getFileInfo(uri).getItemInfo().getSize();
      long stride = fileSize / SAMPLED_RANGE_COUNT;
      ByteArrayOutputStream collectedBytes = new ByteArrayOutputStream();
      for (int sample = 0; sample < SAMPLED_RANGE_COUNT; sample++) {
        long offset = Math.min(sample * stride, fileSize - SAMPLED_RANGE_BYTES);
        collectedBytes.write(readRange(inputStream, offset, SAMPLED_RANGE_BYTES));
      }
      return collectedBytes.toByteArray();
    }
  }

  private static byte[] readRange(
      GoogleCloudStorageInputStream inputStream, long offset, int length) throws IOException {
    inputStream.seek(offset);
    byte[] range = new byte[length];
    int totalBytesRead = 0;
    int bytesRead = 0;
    while (totalBytesRead < length && bytesRead != -1) {
      bytesRead = inputStream.read(range, totalBytesRead, length - totalBytesRead);
      totalBytesRead += Math.max(bytesRead, 0);
    }
    return range;
  }
}
