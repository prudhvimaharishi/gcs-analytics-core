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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.client.GcsReadChannelMetadataExtractor.ExtractedMetadata;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Attribute;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.MetricsRecorder;
import com.google.cloud.gcs.analyticscore.common.telemetry.Operation;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

class GcsReadChannel implements VectoredSeekableByteChannel {
  @FunctionalInterface
  public interface ItemInfoProvider {
    GcsItemInfo getItemInfo(GcsItemId itemId) throws IOException;
  }

  protected Storage storage;
  protected GcsReadOptions readOptions;
  protected volatile GcsItemInfo itemInfo;
  protected volatile GcsItemId itemId;
  private long gcsReadChannelPosition = 0;
  protected Supplier<ExecutorService> executorServiceSupplier;
  private static final ImmutableMap<String, String> COMMON_ATTRIBUTES =
      ImmutableMap.of(Attribute.CLASS_NAME.name(), GcsReadChannel.class.getName());
  // Regular reads are measured by the stream that owns this channel, so their parts record nothing.
  private static final MetricsRecorder NO_OP_RECORDER = (metric, value, attributes) -> {};
  protected final Telemetry telemetry;
  private final ReadStrategy strategy;
  private volatile boolean isGcsReadChannelOpen = true;
  private volatile boolean metadataExtractionAttempted = false;
  protected final ItemInfoProvider itemInfoProvider;

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
    this(storage, itemId, readOptions, executorServiceSupplier, telemetry, null);
  }

  GcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry,
      ItemInfoProvider itemInfoProvider)
      throws IOException {
    checkNotNull(storage, "Storage instance cannot be null");
    checkNotNull(itemId, "Item id cannot be null");
    checkNotNull(executorServiceSupplier, "Thread pool supplier must not be null");
    checkNotNull(telemetry, "Telemetry instance cannot be null");
    this.storage = storage;
    this.itemId = itemId;
    this.readOptions = readOptions;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.itemInfoProvider = itemInfoProvider;
    this.strategy = createReadStrategy(storage, itemId, readOptions, null);
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
    this.storage = storage;
    this.readOptions = readOptions;
    this.itemInfo = itemInfo;
    this.itemId = itemId;
    this.executorServiceSupplier = executorServiceSupplier;
    this.telemetry = telemetry;
    this.itemInfoProvider = null;
    this.strategy = createReadStrategy(storage, itemId, readOptions, itemInfo);
  }

  protected ReadStrategy createReadStrategy(
      Storage storage, GcsItemId itemId, GcsReadOptions readOptions, GcsItemInfo itemInfo)
      throws IOException {
    return new AdaptiveReadStrategy(storage, itemId, readOptions, itemInfo);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    checkChannelOpen();
    if (dst.remaining() == 0) {
      return 0;
    }
    int splitReadLength = getSplitReadLength(dst.remaining());
    if (splitReadLength > 0) {
      return readInParts(dst, splitReadLength);
    }
    int totalBytesRead = 0;
    while (dst.hasRemaining()) {
      int bytesRead = readNextChunk(dst);
      if (bytesRead < 0) {
        return totalBytesRead == 0 ? -1 : totalBytesRead;
      }
      totalBytesRead += bytesRead;
    }

    return totalBytesRead;
  }

  /**
   * Returns how many bytes of a read of {@code requestedLength} bytes to fetch as parallel parts,
   * or 0 if the read is not split. Splitting needs the object size, so no part asks for bytes past
   * the end of the object.
   */
  private int getSplitReadLength(int requestedLength) {
    if (itemInfo == null) {
      return 0;
    }
    long bytesLeft = itemInfo.getSize() - gcsReadChannelPosition;
    if (bytesLeft <= 0) {
      return 0;
    }
    int length = (int) Math.min(requestedLength, bytesLeft);
    return shouldSplit(length) ? length : 0;
  }

  /**
   * Reads {@code length} bytes into {@code dst} as parallel part requests. The parts use their own
   * connections and leave the read strategy untouched, so its position always matches its own
   * connection.
   */
  private int readInParts(ByteBuffer dst, int length) throws IOException {
    ExecutorService executorService = executorServiceSupplier.get();
    checkNotNull(executorService, "Thread pool must not be null");
    ByteBuffer target = dst.duplicate();
    target.limit(target.position() + length);
    recordSplitParts(length);
    try {
      new SplitRangeReader(
              executorService,
              (offset, window) -> readPart(offset, window, NO_OP_RECORDER),
              getSplitThresholdBytes())
          .read(gcsReadChannelPosition, target);
    } catch (IOException | RuntimeException e) {
      rethrowAsFileNotFoundIf404(e);
      throw e;
    }
    dst.position(dst.position() + length);
    gcsReadChannelPosition += length;
    return length;
  }

  private int readNextChunk(ByteBuffer dst) throws IOException {
    int bytesRead;
    try {
      ReadChannel sdkChannel = strategy.getReadChannel(gcsReadChannelPosition, dst.remaining());
      bytesRead = sdkChannel.read(dst);
    } catch (IOException | RuntimeException e) {
      rethrowAsFileNotFoundIf404(e);
      throw e;
    }
    if (bytesRead >= 0) {
      gcsReadChannelPosition += bytesRead;
      strategy.position(gcsReadChannelPosition);
      return bytesRead;
    }
    if (strategy.isEof(gcsReadChannelPosition)) {
      return -1;
    }
    throw createUnexpectedEofException();
  }

  private void checkChannelOpen() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
  }

  private IOException createUnexpectedEofException() {
    long itemSize = itemInfo != null ? itemInfo.getSize() : -1;
    return new IOException(
        String.format(
            "Received end of stream signal before all requestedBytes were received; "
                + "EndOf stream signal received at offset: %d for resource: %s of size: %d",
            gcsReadChannelPosition, itemId, itemSize));
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public long position() throws IOException {
    checkChannelOpen();

    return gcsReadChannelPosition;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    checkChannelOpen();
    validatePosition(newPosition);
    gcsReadChannelPosition = newPosition;

    return this;
  }

  @Override
  public long size() throws IOException {
    if (itemInfo != null || extractMetadataAfterRead(this.strategy)) {
      return itemInfo.getSize();
    }
    if (itemInfoProvider == null) {
      throw new IOException("ItemInfo is not initialized and no ItemInfoProvider was provided.");
    }

    itemInfo = itemInfoProvider.getItemInfo(itemId);
    itemId = itemInfo.getItemId();
    return itemInfo.getSize();
  }

  @Override
  @Nullable
  public GcsItemInfo getItemInfo() {
    return itemInfo;
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    throw new UnsupportedOperationException("Cannot mutate read-only channel");
  }

  @Override
  public boolean isOpen() {
    return isGcsReadChannelOpen;
  }

  @Override
  public void close() throws IOException {
    if (!isGcsReadChannelOpen) {
      return;
    }
    synchronized (this) {
      isGcsReadChannelOpen = false;
      strategy.close();
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
      if (shouldSplit(combinedRange.getLength())) {
        readCombinedRangeInParts(executorService, combinedRange, allocate, operation);
        continue;
      }
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
          ReadStrategy readStrategy =
              new RandomReadStrategy(storage, itemId, readOptions, itemInfo);
          try (ReadChannel channel =
              readStrategy.getReadChannel(
                  combinedObjectRange.getOffset(), combinedObjectRange.getLength())) {
            validatePosition(combinedObjectRange.getOffset());
            ByteBuffer dataBuffer = allocate.apply(combinedObjectRange.getLength());
            if (dataBuffer == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "Buffer allocation returned null for combinedObjectRange: %s",
                      combinedObjectRange));
            }
            int numOfBytesRead = readIntoBuffer(channel, readStrategy, dataBuffer, recorder);
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
            completeWithException(combinedObjectRange, toReadFailure(e));
          }
          return null;
        });
  }

  /**
   * Reads a combined range as parallel part requests that fill one buffer, then completes its
   * underlying ranges with slices of that buffer.
   */
  private void readCombinedRangeInParts(
      ExecutorService executorService,
      GcsObjectCombinedRange combinedRange,
      IntFunction<ByteBuffer> allocate,
      Operation operation) {
    recordSplitParts(combinedRange.getLength());
    SplitRangeReader splitRangeReader =
        new SplitRangeReader(
            executorService,
            (offset, window) ->
                telemetry.measure(
                    operation,
                    recorder -> {
                      readPart(offset, window, recorder);
                      return null;
                    }),
            getSplitThresholdBytes());
    CompletableFuture<ByteBuffer> unused =
        splitRangeReader
            .readAsync(combinedRange.getOffset(), combinedRange.getLength(), allocate)
            .whenComplete(
                (dataBuffer, failure) -> {
                  if (failure != null) {
                    completeWithException(combinedRange, toReadFailure(failure));
                    return;
                  }
                  for (GcsObjectRange underlyingRange : combinedRange.getUnderlyingRanges()) {
                    underlyingRange
                        .getByteBufferFuture()
                        .complete(
                            VectoredIoUtil.fetchUnderlyingRangeData(
                                dataBuffer, combinedRange, underlyingRange));
                  }
                });
  }

  /**
   * Reads object bytes {@code [offset, offset + window.remaining())} into {@code window} through
   * one bounded request.
   */
  private void readPart(long offset, ByteBuffer window, MetricsRecorder recorder)
      throws IOException {
    validatePosition(offset);
    int length = window.remaining();
    ReadStrategy readStrategy = new RandomReadStrategy(storage, itemId, readOptions, itemInfo);
    try (ReadChannel channel = readStrategy.getReadChannel(offset, length)) {
      int numOfBytesRead = readIntoBuffer(channel, readStrategy, window, recorder);
      if (numOfBytesRead < length) {
        throw new EOFException(
            String.format(
                "EOF reached while reading part, offset: %d, item: %s, numRead: %d, expected: %d",
                offset, itemId, numOfBytesRead, length));
      }
    }
  }

  /** Reads from {@code channel} until {@code buffer} is full or the channel ends. */
  private int readIntoBuffer(
      ReadChannel channel, ReadStrategy readStrategy, ByteBuffer buffer, MetricsRecorder recorder)
      throws IOException {
    int numOfBytesRead = 0;
    while (buffer.hasRemaining()) {
      int bytesRead = channel.read(buffer);
      extractMetadataAfterRead(readStrategy);
      if (bytesRead < 0) {
        // EOF reached.
        break;
      }
      recorder.record(Metric.READ_BYTES, bytesRead, Collections.emptyMap());
      numOfBytesRead += bytesRead;
    }
    return numOfBytesRead;
  }

  private boolean shouldSplit(int length) {
    GcsSplitReadOptions splitReadOptions = readOptions.getGcsSplitReadOptions();
    return splitReadOptions.isEnabled() && length > splitReadOptions.getThresholdBytes();
  }

  private int getSplitThresholdBytes() {
    return readOptions.getGcsSplitReadOptions().getThresholdBytes();
  }

  private void recordSplitParts(int length) {
    telemetry.recordMetric(
        Metric.READ_SPLIT_PARTS,
        RangeSplitter.partCount(length, getSplitThresholdBytes()),
        COMMON_ATTRIBUTES);
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
      if (child.getByteBufferFuture().isDone()) {
        continue;
      }
      Throwable failure =
          e instanceof FileNotFoundException
              ? e
              : new IOException(
                  String.format(
                      "Error while populating childRange: %s from combinedRange: %s",
                      child, combinedObjectRange),
                  e);
      child.getByteBufferFuture().completeExceptionally(failure);
    }
  }

  private Throwable toReadFailure(Throwable failure) {
    return isNotFoundStorageException(failure) ? createFileNotFoundException(failure) : failure;
  }

  private static boolean isNotFoundStorageException(Throwable e) {
    if (e instanceof FileNotFoundException) {
      return false;
    }
    return GcsExceptionUtil.getStorageException(e)
        .map(se -> GcsExceptionUtil.getErrorType(se) == GcsExceptionUtil.ErrorType.NOT_FOUND)
        .orElse(false);
  }

  private FileNotFoundException createFileNotFoundException(Throwable cause) {
    FileNotFoundException notFound = GcsExceptionUtil.createFileNotFoundException(itemId);
    notFound.addSuppressed(cause);
    return notFound;
  }

  private void rethrowAsFileNotFoundIf404(Exception e) throws FileNotFoundException {
    if (isNotFoundStorageException(e)) {
      throw createFileNotFoundException(e);
    }
  }

  private void validatePosition(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(
          String.format(
              "Invalid seek offset: position value (%d) must be >= 0 for '%s'", position, itemId));
    }
  }

  private boolean extractMetadataAfterRead(ReadStrategy strategy) {
    if (itemInfo != null || metadataExtractionAttempted) {
      return itemInfo != null;
    }
    synchronized (this) {
      if (itemInfo != null || metadataExtractionAttempted) {
        return itemInfo != null;
      }
      ExtractedMetadata metadata =
          GcsReadChannelMetadataExtractor.extract(strategy.getSdkReadChannel());
      if (metadata != null) {
        updateGcsItemMetadata(metadata);
      }
      metadataExtractionAttempted = true;
      return metadata != null;
    }
  }

  private void updateGcsItemMetadata(ExtractedMetadata metadata) {
    long genToSet =
        metadata.getGeneration() >= 0
            ? metadata.getGeneration()
            : itemId.getContentGeneration().orElse(-1L);

    GcsItemId.Builder itemIdBuilder = GcsItemId.builder().setBucketName(itemId.getBucketName());
    itemId.getObjectName().ifPresent(itemIdBuilder::setObjectName);

    GcsItemInfo.Builder itemInfoBuilder = GcsItemInfo.builder().setSize(metadata.getSize());

    if (genToSet >= 0) {
      itemIdBuilder.setContentGeneration(genToSet);
      itemInfoBuilder.setContentGeneration(genToSet);
    }

    itemId = itemIdBuilder.build();
    itemInfo = itemInfoBuilder.setItemId(itemId).build();
  }
}
