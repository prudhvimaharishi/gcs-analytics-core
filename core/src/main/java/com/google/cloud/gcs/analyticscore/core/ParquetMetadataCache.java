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

import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMetadataCache {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataCache.class);
  private static final String INDEX_DB_NAME = "parquet_metadata_index.db";

  private final String baseGcsPath;
  private final Path localCacheDir;
  private final Storage storage;
  private final List<String> indexFiles;
  private final Telemetry telemetry;

  // Cache for SQLite connections to local index files
  private final LoadingCache<String, Connection> connectionCache;

  private static ParquetMetadataCache instance;

  static void setInstance(ParquetMetadataCache cache) {
    instance = cache;
  }

  private ParquetMetadataCache(
      String baseGcsPath, String localCacheDirPrefix, Telemetry telemetry) {
    this(
        baseGcsPath,
        localCacheDirPrefix,
        telemetry,
        StorageOptions.getDefaultInstance().getService());
  }

  ParquetMetadataCache(
      String baseGcsPath, String localCacheDirPrefix, Telemetry telemetry, Storage storage) {
    this.baseGcsPath = baseGcsPath.endsWith("/") ? baseGcsPath : baseGcsPath + "/";
    this.localCacheDir =
        Paths.get(System.getProperty("java.io.tmpdir"), localCacheDirPrefix, "parquet_cache");
    this.storage = storage;
    this.telemetry = telemetry;

    try {
      LOG.info("Footer index local storage directory: {}", this.localCacheDir);
      Files.createDirectories(this.localCacheDir);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create local cache directory: " + this.localCacheDir, e);
    }

    this.indexFiles = loadIndexFiles();
    LOG.info("Loaded index files: {}", indexFiles);

    this.connectionCache =
        CacheBuilder.newBuilder()
            .maximumSize(100) // Maximum number of connections to cache
            .build(
                new CacheLoader<String, Connection>() {
                  @Override
                  public Connection load(String localDbPath) throws SQLException {
                    return DriverManager.getConnection("jdbc:sqlite:" + localDbPath);
                  }
                });
  }

  private void deleteDirectory(java.io.File directory) throws IOException {
    if (!directory.exists()) {
      return;
    }
    java.io.File[] files = directory.listFiles();
    if (files != null) {
      for (java.io.File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          if (!file.delete()) {
            LOG.warn("Failed to delete file: {}", file);
          }
        }
      }
    }
    if (!directory.delete()) {
      LOG.warn("Failed to delete directory: {}", directory);
    }
  }

  public static synchronized ParquetMetadataCache getInstance(
      GcsItemId itemId, String localCacheDirPrefix, Telemetry telemetry) {
    String baseGcsPath = "gs://" + itemId.getBucketName();
    if (itemId.getBucketName().equals("gcs-hyd-iceberg-benchmark-warehouse")) {
      baseGcsPath =
          "gs://"
              + itemId.getBucketName()
              + "/"
              + getBaseWarehousePath(itemId.getObjectName().orElse(""));
    }
    if (instance == null) {
      instance = new ParquetMetadataCache(baseGcsPath, localCacheDirPrefix, telemetry);
    } else {
      if (!instance.baseGcsPath.equals(
          baseGcsPath.endsWith("/") ? baseGcsPath : baseGcsPath + "/")) {
        LOG.warn(
            "ParquetMetadataCache already initialized with baseGcsPath: {}, initializing with new path: {}",
            instance.baseGcsPath,
            baseGcsPath);
        instance = new ParquetMetadataCache(baseGcsPath, localCacheDirPrefix, telemetry);
      }
    }
    return instance;
  }

  private static String getBaseWarehousePath(String gcsPath) {
    if (gcsPath == null || gcsPath.isEmpty()) {
      return gcsPath;
    }

    // Find the first '/'
    int firstSlash = gcsPath.indexOf('/');
    if (firstSlash == -1) {
      // Path has no slashes, return as-is
      return gcsPath;
    }

    // Find the second '/' starting right after the first one
    int secondSlash = gcsPath.indexOf('/', firstSlash + 1);
    if (secondSlash == -1) {
      // Path has only one slash, return as-is
      return gcsPath;
    }

    // Return the substring up to, but not including, the second slash
    return gcsPath.substring(0, secondSlash);
  }

  private List<String> loadIndexFiles() {
    String bucketName = getBucketName(baseGcsPath);
    String prefix = getPathWithoutBucket(baseGcsPath + "**" + INDEX_DB_NAME);

    List<String> indices = new ArrayList<>();
    try {
      telemetry.measure(
          Operation.FOOTER_INDEX_LIST.name(),
          Metric.FOOTER_INDEX_LIST_DURATION,
          Collections.emptyMap(),
          recorder -> {
            storage
                .list(bucketName, Storage.BlobListOption.matchGlob(prefix))
                .iterateAll()
                .forEach(
                    blob -> {
                      if (blob.getName().endsWith(INDEX_DB_NAME)) {
                        indices.add("gs://" + bucketName + "/" + blob.getName());
                      }
                    });
            recorder.record(Metric.FOOTER_INDEX_LIST_COUNT, 1, Collections.emptyMap());
            return null;
          });
    } catch (Exception e) {
      LOG.error("Error listing index files in {}: {}", baseGcsPath, e.getMessage(), e);
    }
    return ImmutableList.copyOf(indices);
  }

  private Optional<String> findNearestIndex(String objectGcsPath) {
    if (!objectGcsPath.startsWith(baseGcsPath)) {
      return Optional.empty();
    }

    String bucketName = getBucketName(objectGcsPath);
    String relativePath = objectGcsPath.substring(("gs://" + bucketName + "/").length());

    Path currentPath = Paths.get(relativePath).getParent();
    while (currentPath != null && !currentPath.toString().isEmpty()) {
      String indexGcsPath = "gs://" + bucketName + "/" + currentPath.resolve(INDEX_DB_NAME);
      Path localPath =
          localCacheDir.resolve(bucketName).resolve(currentPath.resolve(INDEX_DB_NAME));
      if (Files.exists(localPath) || indexFiles.contains(indexGcsPath)) {
        return Optional.of(indexGcsPath);
      }
      if (("gs://" + bucketName + "/" + currentPath).length() <= baseGcsPath.length()) {
        break;
      }
      currentPath = currentPath.getParent();
    }
    // Check base path itself
    String baseIndex = baseGcsPath + INDEX_DB_NAME;
    Path baseLocalPath = localCacheDir.resolve(bucketName).resolve(getPathWithoutBucket(baseIndex));
    if (Files.exists(baseLocalPath) || indexFiles.contains(baseIndex)) {
      return Optional.of(baseIndex);
    }

    return Optional.empty();
  }

  private Path downloadIndex(String indexGcsPath) throws IOException {
    String bucketName = getBucketName(indexGcsPath);
    String blobName = getPathWithoutBucket(indexGcsPath);

    Path localPath = localCacheDir.resolve(bucketName).resolve(blobName);

    if (!Files.exists(localPath)) {
      synchronized (this) {
        if (!Files.exists(localPath)) {
          LOG.info(
              "No local cache available for index. Downloading {} to {}", indexGcsPath, localPath);
          Files.createDirectories(localPath.getParent());
          Blob blob = storage.get(bucketName, blobName);
          if (blob != null && blob.exists()) {
            Path tempPath =
                localPath.resolveSibling(
                    localPath.getFileName() + "." + java.util.UUID.randomUUID() + ".tmp");
            try {
              telemetry.measure(
                  Operation.FOOTER_INDEX_DOWNLOAD.name(),
                  Metric.FOOTER_INDEX_DOWNLOAD_DURATION,
                  Collections.emptyMap(),
                  recorder -> {
                    blob.downloadTo(tempPath);
                    recorder.record(Metric.FOOTER_INDEX_DOWNLOAD_COUNT, 1, Collections.emptyMap());
                    recorder.record(
                        Metric.FOOTER_INDEX_DOWNLOAD_BYTES, blob.getSize(), Collections.emptyMap());
                    return null;
                  });

              try {
                Files.move(tempPath, localPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                LOG.info("Successfully downloaded and renamed {} to {}", indexGcsPath, localPath);
              } catch (java.nio.file.FileAlreadyExistsException e) {
                LOG.info(
                    "Another process already downloaded the file. Using existing cached file.");
                Files.deleteIfExists(tempPath);
              }
            } catch (Exception e) {
              Files.deleteIfExists(tempPath);
              throw e;
            }
          } else {
            throw new IOException("Index file not found in GCS: " + indexGcsPath);
          }
        }
      }
    } else {
      LOG.info("Index file already cached: {}", localPath);
    }
    return localPath;
  }

  public Optional<ParquetObjectMetadata> getMetadata(String objectGcsPath) {
    Optional<ParquetObjectMetadata> result = Optional.empty();
    Optional<String> indexGcsPathOpt = findNearestIndex(objectGcsPath);
    if (!indexGcsPathOpt.isPresent()) {
      LOG.warn("No index file found for {}", objectGcsPath);
      return Optional.empty();
    }

    String indexGcsPath = indexGcsPathOpt.get();
    try {
      Path localIndexPatn = downloadIndex(indexGcsPath);
      Connection conn = connectionCache.get(localIndexPatn.toString());

      String objectName = BlobId.fromGsUtilUri(objectGcsPath).getName();

      String query =
          "SELECT file_size, footer_length, raw_metadata FROM metadata_index WHERE object_name = ?";
      try (PreparedStatement pstmt = conn.prepareStatement(query)) {
        pstmt.setString(1, objectName);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
          long fileSize = rs.getLong("file_size");
          int footerLength = rs.getInt("footer_length");
          byte[] rawMetadata = rs.getBytes("raw_metadata");
          result =
              Optional.of(
                  new ParquetObjectMetadata(objectName, fileSize, footerLength, rawMetadata));
        } else {
          LOG.info("Metadata not found for {} in {}", objectName, indexGcsPath);
        }
      }
    } catch (IOException e) {
      LOG.error("Error downloading index file {}: {}", indexGcsPath, e.getMessage(), e);
    } catch (SQLException e) {
      LOG.error("SQLite error accessing {}: {}", indexGcsPath, e.getMessage(), e);
    } catch (ExecutionException e) {
      LOG.error("Error getting connection from cache for {}: {}", indexGcsPath, e.getMessage(), e);
    }
    return result;
  }

  private String getBucketName(String gcsPath) {
    return gcsPath.substring(5).split("/")[0];
  }

  private String getPathWithoutBucket(String gcsPath) {
    String noGsPrefix = gcsPath.substring(5);
    return noGsPrefix.substring(noGsPrefix.indexOf('/') + 1);
  }

  public static class ParquetObjectMetadata {
    private final String objectName;
    private final long fileSize;
    private final int footerLength;
    private final byte[] rawMetadata;

    public ParquetObjectMetadata(
        String objectName, long fileSize, int footerLength, byte[] rawMetadata) {
      this.objectName = objectName;
      this.fileSize = fileSize;
      this.footerLength = footerLength;
      this.rawMetadata = rawMetadata;
    }

    public String getObjectName() {
      return objectName;
    }

    public long getFileSize() {
      return fileSize;
    }

    public int getFooterLength() {
      return footerLength;
    }

    public byte[] getRawMetadata() {
      return rawMetadata;
    }
  }
}
