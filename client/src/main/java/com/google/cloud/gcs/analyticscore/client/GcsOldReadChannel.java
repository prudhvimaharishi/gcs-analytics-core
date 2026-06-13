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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;

class GcsOldReadChannel implements VectoredSeekableByteChannel {
  private Storage storage;
  private GcsReadOptions readOptions;
  private ReadChannel readChannel;
  protected GcsItemInfo itemInfo;
  protected GcsItemId itemId;
  private long position = 0;
  private Supplier<ExecutorService> executorServiceSupplier;
  private static final ImmutableMap<String, String> COMMON_ATTRIBUTES =
      ImmutableMap.of(Attribute.CLASS_NAME.name(), GcsOldReadChannel.class.getName());

  private final Telemetry telemetry;

  GcsOldReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(
        storage,
        itemInfo,
        checkNotNull(itemInfo, "Item info cannot be null").getItemId(),
        readOptions,
        executorServiceSupplier,
        telemetry);
  }

  GcsOldReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, null, itemId, readOptions, executorServiceSupplier, telemetry);
  }

  private GcsOldReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    checkNotNull(storage, "Storage instance cannot be null");
    checkNotNull(itemId, "Item id cannot be null");
    checkNotNull(executorServiceSupplier, "Thread pool supplier must not be null");
    checkNotNull(telemetry, "Telemetry instance cannot be null");
    this.storage = storage;
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.itemId = itemId;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.readChannel = openReadChannel(itemId, readOptions);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    Map<String, String> telemetryAttributes =
        ImmutableMap.<String, String>builder()
            .putAll(COMMON_ATTRIBUTES)
            .put(Attribute.READ_LENGTH.name(), String.valueOf(dst.remaining()))
            .put(Attribute.READ_OFFSET.name(), String.valueOf(dst.position()))
            .build();
    return telemetry.measure(
        GcsAnalyticsCoreTelemetryConstants.Operation.READ.name(),
        Metric.READ_DURATION,
        telemetryAttributes,
        recorder -> {
          int bytesRead = readChannel.read(dst);
          if (bytesRead > 0) {
            position += bytesRead;
            recorder.record(Metric.READ_BYTES, bytesRead, Collections.emptyMap());
          }
          return bytesRead;
        });
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    validatePosition(newPosition);
    if (newPosition != position) {
      long seekDistance = Math.abs(newPosition - position);
      telemetry.measure(
          GcsAnalyticsCoreTelemetryConstants.Operation.SEEK.name(),
          Metric.HARD_SEEK_DURATION,
          COMMON_ATTRIBUTES,
          recorder -> {
            readChannel.seek(newPosition);
            recorder.record(Metric.SEEK_DISTANCE, seekDistance, Collections.emptyMap());
            recorder.record(Metric.HARD_SEEK_COUNT, 1, Collections.emptyMap());
            recorder.record(Metric.HARD_SEEK_BYTES, seekDistance, Collections.emptyMap());
            return null;
          });
      position = newPosition;
    }

    return this;
  }

  @Override
  public long size() throws IOException {
    if (null != itemInfo) {
      return itemInfo.getSize();
    }
    throw new IOException("Object metadata not initialized");
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return readChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (readChannel.isOpen()) {
      telemetry.measure(
          GcsAnalyticsCoreTelemetryConstants.Operation.CLOSE.name(),
          Metric.CLOSE_DURATION,
          COMMON_ATTRIBUTES,
          recorder -> {
            readChannel.close();
            return null;
          });
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate)
      throws IOException {
    Operation operation =
        Operation.builder()
            .setName(GcsAnalyticsCoreTelemetryConstants.Operation.VECTORED_READ.name())
            .setDurationMetric(Metric.READ_DURATION)
            .setAttributes(COMMON_ATTRIBUTES)
            .build();
    ExecutorService executorService = executorServiceSupplier.get();
    checkNotNull(executorService, "Thread pool must not be null");
    GcsVectoredReadOptions vectoredReadOptions = readOptions.getGcsVectoredReadOptions();
    ImmutableList<GcsObjectCombinedRange> combinedRanges =
        VectoredIoUtil.mergeGcsObjectRanges(
            ImmutableList.copyOf(ranges),
            vectoredReadOptions.getMaxMergeGap(),
            vectoredReadOptions.getMaxMergeSize());

    for (GcsObjectCombinedRange combinedRange : combinedRanges) {
      var unused =
          executorService.submit(
              () -> {
                readCombinedRange(combinedRange, allocate, operation);
              });
    }
  }

  void readCombinedRange(
      GcsObjectCombinedRange combinedObjectRange,
      IntFunction<ByteBuffer> allocate,
      Operation operation) {
    telemetry.measure(
        operation,
        recorder -> {
          try (ReadChannel channel = openReadChannel(itemId, readOptions)) {
            validatePosition(combinedObjectRange.getOffset());
            channel.seek(combinedObjectRange.getOffset());
            channel.limit(combinedObjectRange.getOffset() + combinedObjectRange.getLength());
            ByteBuffer dataBuffer = allocate.apply(combinedObjectRange.getLength());
            int numOfBytesRead = 0;
            while (dataBuffer.hasRemaining()) {
              int bytesRead = channel.read(dataBuffer);
              if (bytesRead < 0) {
                // EOF reached.
                break;
              }
              recorder.record(Metric.READ_BYTES, bytesRead, Collections.emptyMap());
              numOfBytesRead += bytesRead;
            }
            if (numOfBytesRead < combinedObjectRange.getLength()) {
              throw new EOFException(
                  String.format(
                      "EOF reached while reading combinedObjectRange, range: %s, item: "
                          + "%s, numRead: %d, expected: %d",
                      combinedObjectRange,
                      itemId,
                      numOfBytesRead,
                      combinedObjectRange.getLength()));
            }
            // making it ready for reading
            dataBuffer.flip();
            for (GcsObjectRange underlyingRange : combinedObjectRange.getUnderlyingRanges()) {
              populateGcsObjectRangeFromCombinedObjectRange(
                  combinedObjectRange, underlyingRange, numOfBytesRead, dataBuffer);
            }
          } catch (Exception e) {
            completeWithException(combinedObjectRange, e);
          }
          return null;
        });
  }

  private void populateGcsObjectRangeFromCombinedObjectRange(
      GcsObjectCombinedRange combinedObjectRange,
      GcsObjectRange objectRange,
      long numOfBytesRead,
      ByteBuffer dataBuffer)
      throws EOFException {
    long maxPosition = combinedObjectRange.getOffset() + numOfBytesRead;
    long objectRangeEndPosition = objectRange.getOffset() + objectRange.getLength();
    if (objectRangeEndPosition <= maxPosition) {
      ByteBuffer childBuffer =
          VectoredIoUtil.fetchUnderlyingRangeData(dataBuffer, combinedObjectRange, objectRange);
      objectRange.getByteBufferFuture().complete(childBuffer);
    } else {
      throw new EOFException(
          String.format(
              "EOF reached before all child ranges can be populated, "
                  + "combinedObjectRange: %s, "
                  + "expected length: %s, readBytes: %s, path: %s",
              combinedObjectRange, combinedObjectRange.getLength(), numOfBytesRead, itemId));
    }
  }

  private void completeWithException(GcsObjectCombinedRange combinedObjectRange, Throwable e) {
    for (GcsObjectRange child : combinedObjectRange.getUnderlyingRanges()) {
      if (!child.getByteBufferFuture().isDone()) {
        child
            .getByteBufferFuture()
            .completeExceptionally(
                new IOException(
                    String.format(
                        "Error while populating childRange: %s from combinedRange: %s",
                        child, combinedObjectRange),
                    e));
      }
    }
  }

  protected ReadChannel openReadChannel(GcsItemId gcsItemId, GcsReadOptions readOptions)
      throws IOException {
    checkArgument(gcsItemId.isGcsObject(), "Expected Gcs Object but got %s", gcsItemId);
    String bucketName = gcsItemId.getBucketName();
    String objectName = gcsItemId.getObjectName().get();
    BlobId blobId =
        gcsItemId
            .getContentGeneration()
            .map(gen -> BlobId.of(bucketName, objectName, gen))
            .orElse(BlobId.of(bucketName, objectName));
    List<Storage.BlobSourceOption> sourceOptions = Lists.newArrayList();
    readOptions
        .getUserProjectId()
        .ifPresent(id -> sourceOptions.add(Storage.BlobSourceOption.userProject(id)));
    readOptions
        .getDecryptionKey()
        .ifPresent(key -> sourceOptions.add(Storage.BlobSourceOption.decryptionKey(key)));

    return telemetry.measure(
        GcsAnalyticsCoreTelemetryConstants.Operation.OPEN.name(),
        Metric.OPEN_DURATION,
        COMMON_ATTRIBUTES,
        recorder -> {
          ReadChannel readChannel =
              storage.reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
          recorder.record(Metric.CHANNEL_OPEN_COUNT, 1, Collections.emptyMap());
          readOptions.getChunkSize().ifPresent(readChannel::setChunkSize);
          return readChannel;
        });
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'", position, itemId));
    }
  }
}
