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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;

class GcsReadChannel implements VectoredSeekableByteChannel {
  private static final Logger LOG = LoggerFactory.getLogger(GcsReadChannel.class);
  private Storage storage;
  private GcsReadOptions readOptions;
  protected GcsItemInfo itemInfo;
  protected GcsItemId itemId;
  private long position = 0;
  private Supplier<ExecutorService> executorServiceSupplier;
  private static final ImmutableMap<String, String> COMMON_ATTRIBUTES =
      ImmutableMap.of(Attribute.CLASS_NAME.name(), GcsReadChannel.class.getName());

  private final Telemetry telemetry;

  // Adaptive range read state
  private AdaptiveReadStrategy strategy;
  private long sessionPosition = -1;
  private long sessionEnd = -1;
  private ReadChannel sessionReadChannel = null;
  private byte[] skipBuffer = null;
  private static final int SKIP_BUFFER_SIZE = 8192;
  private boolean open = true;

  GcsReadChannel(
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

  GcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    this(storage, null, itemId, readOptions, executorServiceSupplier, telemetry);
  }

  private GcsReadChannel(
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
    checkArgument(itemId.isGcsObject(), "Expected Gcs Object but got %s", itemId);
    
    this.storage = storage;
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.itemId = itemId;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;

    this.strategy = new AdaptiveReadStrategy(readOptions);
    this.sessionReadChannel = openBoundedReadChannel(0);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (!open) {
      throw new java.nio.channels.ClosedChannelException();
    }
    if (dst.remaining() == 0) {
      return 0;
    }
    performPendingSeeks();
    int totalBytesRead = 0;
    while (dst.hasRemaining()) {
      try {
        if (sessionReadChannel == null) {
          sessionReadChannel = openBoundedReadChannel(dst.remaining());
        }
        int bytesRead = sessionReadChannel.read(dst);
        if (bytesRead < 0) {
          if (itemInfo != null && position == itemInfo.getSize()) {
            if (totalBytesRead == 0) {
              return -1;
            }
            break;
          }
          if (position == sessionEnd) {
            closeSession();
            continue;
          }
          if (itemInfo != null) {
            throw new IOException(
                String.format(
                    "Received end of stream result before all requestedBytes were received;"
                        + "EndOf stream signal received at offset: %d where as stream was suppose to end at: %d for resource: %s of size: %d",
                    position, sessionEnd, itemId, itemInfo.getSize()));
          } else {
             // If itemInfo is null, we rely on bytesRead < 0 to mean EOF.
             if (totalBytesRead == 0) {
               return -1;
             }
             break;
          }
        }
        totalBytesRead += bytesRead;
        position += bytesRead;
        sessionPosition += bytesRead;
      } catch (IOException e) {
        closeSession();
        throw e;
      }
    }
    return totalBytesRead;
  }

  private ReadChannel openBoundedReadChannel(long bytesToRead) throws IOException {
    sessionPosition = position;
    strategy.detectSequentialAccess(sessionPosition);
    long size = (itemInfo != null) ? itemInfo.getSize() : Long.MAX_VALUE;
    sessionEnd = strategy.calculateAdaptiveReadSessionEnd(sessionPosition, bytesToRead, size);
    ReadChannel channel = openReadChannel(itemId, readOptions);
    try {
      channel.seek(sessionPosition);
      if (sessionEnd != Long.MAX_VALUE) {
        channel.limit(sessionEnd);
      }
      return channel;
    } catch (Exception e) {
      throw new IOException(
          String.format("Unable to update the boundaries/Range of contentChannel %s", itemId), e);
    }
  }

  private void performPendingSeeks() throws IOException {
    if (sessionReadChannel != null && position == sessionPosition) {
      return;
    }
    if (canSeekInPlace()) {
      skipInPlace();
      return;
    }
    strategy.detectRandomAccess(position, sessionPosition);
    closeSession();
  }

  private boolean canSeekInPlace() {
    return sessionReadChannel != null
        && strategy.shouldSeekInPlace(position, sessionPosition, sessionEnd);
  }

  private void skipInPlace() throws IOException {
    if (skipBuffer == null) {
      skipBuffer = new byte[SKIP_BUFFER_SIZE];
    }
    long seekDistance = position - sessionPosition;
    while (seekDistance > 0 && sessionReadChannel != null) {
      try {
        int bufferSize = (int) Math.min((long) skipBuffer.length, seekDistance);
        int bytesRead = sessionReadChannel.read(ByteBuffer.wrap(skipBuffer, 0, bufferSize));
        if (bytesRead < 0) {
          closeSession();
          return;
        }
        seekDistance -= bytesRead;
        sessionPosition += bytesRead;
      } catch (IOException e) {
        closeSession();
        throw e;
      }
    }
  }

  private void closeSession() {
    if (sessionReadChannel != null) {
      try {
        sessionReadChannel.close();
      } catch (Exception e) {
        LOG.debug("Got an exception on closing AdaptiveReadChannelSession for '{}'; ignoring it.", itemId, e);
      } finally {
        sessionReadChannel = null;
        sessionPosition = -1;
        sessionEnd = -1;
      }
    }
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
    position = newPosition;

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
    return open;
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
      closeSession();
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
    ReadChannel readChannel =
        storage.reader(blobId, sourceOptions.toArray(new Storage.BlobSourceOption[0]));
    readOptions.getChunkSize().ifPresent(readChannel::setChunkSize);

    return readChannel;
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'", position, itemId));
    }
  }
}
