/*
 * Copyright 2025 Google LLC
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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.LoggingTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.OpenTelemetryReporter;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.cloud.storage.BlobId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

public class GcsFileSystemImpl implements GcsFileSystem {

  /**
   * Using a 30-second keep-alive enables efficient thread reuse during intermittent spikes in
   * status and list requests, while ensuring rapid resource cleanup during periods of inactivity.
   */
  private static final int CACHED_EXECUTOR_KEEP_ALIVE_SECONDS = 30;

  // TODO: Determine the appropriate values for pool size and queue capacity for cached executor by
  // benchmarking.
  /** Max thread pool size for cached status executor, clamped between 16 and 128. */
  private static final int CACHED_EXECUTOR_MAX_POOL_SIZE =
      Math.max(16, Math.min(Runtime.getRuntime().availableProcessors() * 4, 128));

  /** Timeout in seconds for background thread pools to terminate when closing the file system. */
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10;

  private final GcsClient gcsClient;
  private final GcsFileSystemOptions fileSystemOptions;
  private final Supplier<ExecutorService> readExecutorServiceSupplier;
  private final Supplier<ExecutorService> statusExecutorServiceSupplier;
  private final Telemetry telemetry;
  private final AnalyticsCacheManager cacheManager;
  private final FlatNamespaceStrategyImpl flatStrategy;
  private final HierarchicalNamespaceStrategyImpl hnsStrategy;
  @Nullable private final GcsClientCache clientCache;
  @Nullable private final GcsClientCache.Lease clientLease;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Creates a file system whose credentials are resolved from {@code fileSystemOptions}.
   *
   * @param fileSystemOptions The file system options, including the authentication options.
   * @throws UncheckedIOException If credentials cannot be resolved from the authentication options.
   */
  public GcsFileSystemImpl(GcsFileSystemOptions fileSystemOptions) {
    this(
        checkNotNull(fileSystemOptions, "fileSystemOptions should not be null"),
        /* credentials= */ null,
        /* hasExplicitCredentials= */ false);
  }

  /**
   * Creates a file system that uses {@code credentials} instead of resolving credentials from the
   * authentication options in {@code fileSystemOptions}.
   *
   * @param credentials The credentials for GCS requests.
   * @param fileSystemOptions The file system options.
   */
  public GcsFileSystemImpl(Credentials credentials, GcsFileSystemOptions fileSystemOptions) {
    this(
        checkNotNull(fileSystemOptions, "fileSystemOptions should not be null"),
        checkNotNull(credentials, "credentials should not be null"),
        /* hasExplicitCredentials= */ true);
  }

  private GcsFileSystemImpl(
      GcsFileSystemOptions fileSystemOptions,
      @Nullable Credentials credentials,
      boolean hasExplicitCredentials) {
    this.fileSystemOptions = fileSystemOptions;
    this.statusExecutorServiceSupplier = initializeStatusExecutionServiceSupplier();
    this.telemetry = createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions());
    this.cacheManager = new AnalyticsCacheManager(fileSystemOptions.getGcsCacheOptions());
    this.clientCache = GcsClientCache.getInstance();
    this.clientLease =
        telemetry.measure(
            GcsAnalyticsCoreTelemetryConstants.Operation.GCS_CLIENT_CREATE.name(),
            GcsAnalyticsCoreTelemetryConstants.Metric.GCS_CLIENT_CREATE_DURATION,
            Collections.emptyMap(),
            recorder ->
                hasExplicitCredentials
                    ? clientCache.acquire(
                        credentials,
                        fileSystemOptions,
                        this::initializeReadExecutionServiceSupplier,
                        telemetry)
                    : clientCache.acquire(
                        fileSystemOptions,
                        this::initializeReadExecutionServiceSupplier,
                        telemetry));
    this.readExecutorServiceSupplier = this.clientLease.readExecutorServiceSupplier();
    this.gcsClient = this.clientLease.client();
    this.flatStrategy = new FlatNamespaceStrategyImpl(this.gcsClient);
    this.hnsStrategy = new HierarchicalNamespaceStrategyImpl(this.gcsClient);
  }

  @VisibleForTesting
  GcsFileSystemImpl(GcsClient gcsClient, GcsFileSystemOptions fileSystemOptions) {
    this(
        gcsClient,
        fileSystemOptions,
        createTelemetry(fileSystemOptions.getAnalyticsCoreTelemetryOptions()),
        new AnalyticsCacheManager(fileSystemOptions.getGcsCacheOptions()));
  }

  @VisibleForTesting
  GcsFileSystemImpl(
      GcsClient gcsClient,
      GcsFileSystemOptions fileSystemOptions,
      Telemetry telemetry,
      AnalyticsCacheManager cacheManager) {
    this.gcsClient = gcsClient;
    this.fileSystemOptions = fileSystemOptions;
    this.readExecutorServiceSupplier = initializeReadExecutionServiceSupplier();
    this.statusExecutorServiceSupplier = initializeStatusExecutionServiceSupplier();
    this.telemetry = telemetry;
    this.cacheManager = cacheManager;
    this.clientCache = null;
    this.clientLease = null;
    this.flatStrategy = new FlatNamespaceStrategyImpl(this.gcsClient);
    this.hnsStrategy = new HierarchicalNamespaceStrategyImpl(this.gcsClient);
  }

  @VisibleForTesting
  NamespaceStrategy resolveStrategy(String bucketName) throws IOException {
    checkNotNull(bucketName, "bucketName cannot be null");
    if (!fileSystemOptions.isHnsApiEnabled()) {
      return flatStrategy;
    }

    BucketProperties properties =
        cacheManager.getBucketProperties(
            bucketName, name -> BucketProperties.create(gcsClient.isHnsBucket(name)));

    if (properties.isHnsEnabled()) {
      return hnsStrategy;
    }
    return flatStrategy;
  }

  @Override
  public VectoredSeekableByteChannel open(GcsFileInfo gcsFileInfo, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsFileInfo, "fileInfo should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(gcsFileInfo.getUri().toString());
    checkArgument(itemId.isGcsObject(), "Expected GCS object to be provided. But got: " + itemId);
    if (gcsClient instanceof GcsClientImpl) {
      return ((GcsClientImpl) gcsClient)
          .openReadChannel(gcsFileInfo.getItemInfo(), readOptions, telemetry);
    }
    return gcsClient.openReadChannel(gcsFileInfo.getItemInfo(), readOptions);
  }

  @Override
  public VectoredSeekableByteChannel open(GcsItemId gcsItemId, GcsReadOptions readOptions)
      throws IOException {
    checkNotNull(gcsItemId, "gcsItemId should not be null");
    checkArgument(
        gcsItemId.isGcsObject(), "Expected GCS object to be provided. But got: " + gcsItemId);
    if (gcsClient instanceof GcsClientImpl) {
      return ((GcsClientImpl) gcsClient).openReadChannel(gcsItemId, readOptions, telemetry);
    }
    return gcsClient.openReadChannel(gcsItemId, readOptions);
  }

  @Override
  public GcsFileInfo getFileInfo(URI path) throws IOException {
    checkNotNull(path, "path should not be null");
    GcsItemId itemId = UriUtil.getItemIdFromString(path.toString());
    return getFileInfo(itemId);
  }

  @Override
  public GcsFileInfo getFileInfo(GcsItemId itemId) throws IOException {
    GcsItemInfo gcsItemInfo = gcsClient.getGcsItemInfo(itemId);
    return GcsFileInfo.builder()
        .setItemInfo(gcsItemInfo)
        .setUri(
            URI.create(
                BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()).toGsUtilUri()))
        .setAttributes(Collections.emptyMap())
        .build();
  }

  @Override
  public GcsFileSystemOptions getFileSystemOptions() {
    return this.fileSystemOptions;
  }

  @Override
  public GcsClient getGcsClient() {
    return this.gcsClient;
  }

  @Override
  public Telemetry getTelemetry() {
    return telemetry;
  }

  @Override
  public AnalyticsCacheManager getCacheManager() {
    return cacheManager;
  }

  @VisibleForTesting
  FlatNamespaceStrategyImpl getFlatStrategy() {
    return flatStrategy;
  }

  @VisibleForTesting
  HierarchicalNamespaceStrategyImpl getHnsStrategy() {
    return hnsStrategy;
  }

  @VisibleForTesting
  Supplier<ExecutorService> getReadExecutorServiceSupplier() {
    return readExecutorServiceSupplier;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (clientLease != null) {
      shutdownExecutorService(statusExecutorServiceSupplier.get());
      clientCache.release(clientLease);
      telemetry.close();
      return;
    }
    ExecutorService readExecutorService = readExecutorServiceSupplier.get();
    ExecutorService statusExecutorService = statusExecutorServiceSupplier.get();
    readExecutorService.shutdown();
    statusExecutorService.shutdown();
    try {
      // Wait a total of EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS for both thread pools to terminate.
      long deadline =
          System.nanoTime() + TimeUnit.SECONDS.toNanos(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
      // First, wait for the read executor service to terminate.
      if (!readExecutorService.awaitTermination(
          EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        readExecutorService.shutdownNow();
      }
      // Then, wait for the status executor service to terminate, with the remaining time.
      if (!statusExecutorService.awaitTermination(
          Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
        statusExecutorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      readExecutorService.shutdownNow();
      statusExecutorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    gcsClient.close();
    telemetry.close();
  }

  static void shutdownExecutorService(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public WritableByteChannel create(GcsItemId itemId, GcsWriteOptions writeOptions)
      throws IOException {
    checkNotNull(itemId, "itemId should not be null");

    // Delegate the actual SDK interaction and exception handling to the internal client
    return gcsClient.createWriteChannel(itemId, writeOptions);
  }

  @VisibleForTesting
  static Telemetry createTelemetry(TelemetryOptions telemetryOptions) {
    ImmutableList.Builder<OperationListener> listeners = ImmutableList.builder();
    telemetryOptions
        .getLoggingTelemetryOptions()
        .filter(LoggingTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new LoggingTelemetryReporter(options)));
    telemetryOptions
        .getOpenTelemetryOptions()
        .filter(OpenTelemetryOptions::isEnabled)
        .ifPresent(options -> listeners.add(new OpenTelemetryReporter(options)));
    telemetryOptions
        .getCustomTelemetryOptions()
        .ifPresent(options -> listeners.addAll(options.getOperationListeners()));
    return new Telemetry(listeners.build());
  }

  @VisibleForTesting
  Supplier<ExecutorService> initializeReadExecutionServiceSupplier() {
    return Suppliers.memoize(
        () ->
            new ThreadPoolExecutor(
                fileSystemOptions.getReadThreadCount(),
                fileSystemOptions.getReadThreadCount(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder()
                    .setNameFormat("gcs-filesystem-range-pool-%d")
                    .setDaemon(true)
                    .build()));
  }

  @VisibleForTesting
  Supplier<ExecutorService> initializeStatusExecutionServiceSupplier() {
    return Suppliers.memoize(
        () -> {
          if (fileSystemOptions.isMetadataLookupParallelEnabled()) {
            return createCachedExecutor();
          }
          return new LazyExecutorService();
        });
  }

  private static ExecutorService createCachedExecutor() {

    // Setting corePoolSize equal to maxPoolSize combined with allowCoreThreadTimeOut ensures that
    // incoming tasks immediately spawn new threads up to CACHED_EXECUTOR_MAX_POOL_SIZE before
    // tasks are queued in the LinkedBlockingQueue, while allowing all idle threads to terminate
    // after 30 seconds of inactivity.
    // An unbounded queue is used so that new tasks are not rejected due to queue capacity.
    ThreadPoolExecutor service =
        new ThreadPoolExecutor(
            /* corePoolSize= */ CACHED_EXECUTOR_MAX_POOL_SIZE,
            /* maximumPoolSize= */ CACHED_EXECUTOR_MAX_POOL_SIZE,
            /* keepAliveTime= */ CACHED_EXECUTOR_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-filesystem-cached-pool-%d")
                .setDaemon(true)
                .build());
    // allowCoreThreadTimeOut needs to be enabled for cases where the encapsulating class does not
    // properly shut down the executor, preventing thread leaks.
    service.allowCoreThreadTimeOut(true);
    return service;
  }
}
