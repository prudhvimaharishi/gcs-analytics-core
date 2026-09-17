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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.SchemaAccessHistory;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link FormatOptimizer} that speculatively fetches Parquet column data before the engine asks
 * for it.
 *
 * <p>The object is treated as a grid of fixed-size blocks owned by {@link PrefetchBufferCache}.
 * Serving a read is therefore pure arithmetic and works even when the footer cannot be parsed; the
 * Parquet layout is consulted only to decide what to fetch next.
 *
 * <p>Learning uses the exact byte range of each request rather than the block it happens to land
 * in, because a block is large enough to hold columns the query never asked for and fetching those
 * again in every following row group costs more than it saves.
 *
 * <p>Speculation stays strictly ahead of the reader: the block under the cursor is already being
 * streamed by the channel, so refetching it would duplicate bytes already on the wire. When a
 * wanted block turns out to be mid-flight the read waits for it instead of issuing its own copy of
 * the same request.
 *
 * <p>Instances are bound to a single stream and are not thread-safe.
 */
public final class PredictivePrefetchOptimizer implements FormatOptimizer {

  private static final String PARQUET_EXTENSION = ".parquet";
  private static final int FOOTER_READ_BYTES = 1024 * 1024;
  private static final int MAX_CONCURRENT_PREFETCH_BLOCKS = 32;

  /**
   * How long a read waits for a block that is already being fetched before giving up and reading
   * the bytes itself. Bounded so that a prefetch stuck behind a saturated thread pool delays a read
   * rather than stalling it.
   */
  private static final long IN_FLIGHT_WAIT_TIMEOUT_MILLIS = 5_000;

  private final GcsPrefetchOptions prefetchOptions;
  private final Telemetry telemetry;

  private GcsItemId itemId;
  private AnalyticsCacheManager cacheManager;
  private PrefetchBufferCache bufferCache;
  private SchemaAccessHistory accessHistory;
  private PrefetchScheduler scheduler;
  @Nullable private VectoredSeekableByteChannel sourceChannel;

  private long fileSize = -1;
  private boolean layoutLoadAttempted;
  private boolean firstRowGroupPrefetched;
  @Nullable private ParquetFileLayout layout;
  private long[] rowGroupStartOffsets = new long[0];
  private long[] rowGroupEndOffsets = new long[0];
  private int currentRowGroupOrdinal = -1;
  private int pendingSpeculationRowGroupOrdinal = -1;
  private final Set<Long> speculatedBlockOffsets = new HashSet<>();

  public PredictivePrefetchOptimizer(GcsPrefetchOptions prefetchOptions, Telemetry telemetry) {
    this.prefetchOptions = checkNotNull(prefetchOptions, "prefetchOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return prefetchOptions.isEnabled()
        && itemId
            .getObjectName()
            .map(name -> name.toLowerCase().endsWith(PARQUET_EXTENSION))
            .orElse(false);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.itemId = itemId;
    this.cacheManager = cacheManager;
    this.bufferCache = cacheManager.getPrefetchBufferCache();
    this.accessHistory = cacheManager.getSchemaAccessHistory();
    this.scheduler = new PrefetchScheduler(bufferCache, telemetry, MAX_CONCURRENT_PREFETCH_BLOCKS);
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    onOpen(fileInfo.getItemInfo().getItemId(), cacheManager);
    this.fileSize = fileInfo.getItemInfo().getSize();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    this.sourceChannel = source;
    if (fileSize < 0) {
      fileSize = source.size();
    }
    int requestedLength = dst.remaining();
    int servedBytes = serveFromCache(position, dst);

    ensureLayoutLoaded(source);
    if (layout != null) {
      recordColumnsForRead(position, requestedLength);
    }
    return servedBytes;
  }

  /**
   * Fetches what the read that just completed implies about the bytes after it.
   *
   * <p>This runs once the read has been served rather than while serving it, because a speculative
   * request issued first would sit ahead of the caller's own read in the same thread pool.
   */
  @Override
  public void afterRead(long position, VectoredSeekableByteChannel source) {
    if (layout == null) {
      return;
    }
    int rowGroupOrdinal = findRowGroupOrdinal(position);
    if (rowGroupOrdinal < 0) {
      prefetchFirstRowGroupOnce(source);
      return;
    }
    firstRowGroupPrefetched = true;
    if (rowGroupOrdinal != currentRowGroupOrdinal) {
      currentRowGroupOrdinal = rowGroupOrdinal;
      speculatedBlockOffsets.clear();
    }
    long currentBlockOffset = bufferCache.alignDown(position);
    if (!speculatedBlockOffsets.add(currentBlockOffset)) {
      return;
    }
    speculateRowGroups(source, rowGroupOrdinal, currentBlockOffset);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) {
    List<GcsObjectRange> unservedRanges = new ArrayList<>();
    for (GcsObjectRange range : ranges) {
      if (!tryCompleteFromCache(range, allocate)) {
        unservedRanges.add(range);
      }
    }
    return ImmutableList.copyOf(unservedRanges);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source)
      throws IOException {
    this.sourceChannel = source;
    if (fileSize < 0) {
      fileSize = source.size();
    }
    List<GcsObjectRange> unservedRanges = readVectored(ranges, allocate);

    ensureLayoutLoaded(source);
    if (layout != null) {
      recordVectoredAccess(ranges);
    }
    return unservedRanges;
  }

  /**
   * Fetches what the recorded ranges imply about the row group that follows them.
   *
   * <p>A vectored request already spans every column the engine needs from the row groups it
   * touches, so unlike the streaming path there is nothing left worth speculating in those row
   * groups; fetching them again would duplicate bytes already in flight.
   */
  @Override
  public void afterReadVectored(List<GcsObjectRange> ranges, VectoredSeekableByteChannel source) {
    if (layout == null) {
      return;
    }
    if (pendingSpeculationRowGroupOrdinal < 0) {
      prefetchFirstRowGroupOnce(source);
      return;
    }
    firstRowGroupPrefetched = true;
    speculateRowGroup(source, pendingSpeculationRowGroupOrdinal);
    pendingSpeculationRowGroupOrdinal = -1;
  }

  @Override
  public void onClose() {
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private int serveFromCache(long position, ByteBuffer dst) {
    int servedBytes = copyFromCache(position, dst);
    if (servedBytes == 0) {
      servedBytes = awaitInFlightBlockAndCopy(position, dst);
    }
    if (servedBytes == 0) {
      telemetry.recordMetric(Metric.PREFETCH_CACHE_MISS, 1L, Collections.emptyMap());
      return 0;
    }
    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    telemetry.recordMetric(Metric.PREFETCH_BYTES_CONSUMED, servedBytes, Collections.emptyMap());
    return servedBytes;
  }

  /**
   * Copies as many consecutive resident blocks as {@code dst} has room for, starting at {@code
   * position}, and returns the total copied.
   */
  private int copyFromCache(long position, ByteBuffer dst) {
    int servedBytes = 0;
    while (dst.hasRemaining()) {
      int copiedBytes = bufferCache.copyInto(itemId, position + servedBytes, dst);
      if (copiedBytes == 0) {
        break;
      }
      servedBytes += copiedBytes;
    }
    return servedBytes;
  }

  /**
   * Waits for the block holding {@code position} when a speculative request for it is already
   * outstanding, then serves the read from it.
   *
   * <p>Waiting beats reading the same bytes again: the request is typically most of the way done,
   * and a second one would compete with it for the same connection pool.
   */
  private int awaitInFlightBlockAndCopy(long position, ByteBuffer dst) {
    CompletableFuture<ByteBuffer> pending = pendingBlockFuture(position);
    if (pending == null) {
      return 0;
    }
    try {
      ByteBuffer unused = pending.get(IN_FLIGHT_WAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return 0;
    } catch (ExecutionException | TimeoutException | CancellationException e) {
      return 0;
    }
    return copyFromCache(position, dst);
  }

  @Nullable
  private CompletableFuture<ByteBuffer> pendingBlockFuture(long position) {
    if (scheduler == null) {
      return null;
    }
    return scheduler.getPublishedFuture(bufferCache.alignDown(position));
  }

  /**
   * Takes ownership of {@code range} when its bytes are cached or already being fetched, and
   * returns whether the caller can stop treating it as outstanding.
   */
  private boolean tryCompleteFromCache(GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    if (bufferCache == null) {
      return false;
    }
    if (completeFromCache(range, allocate)) {
      return true;
    }
    return completeWhenInFlightBlocksArrive(range, allocate);
  }

  /** Completes {@code range} from resident blocks, or returns false if any of them is absent. */
  private boolean completeFromCache(GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    if (!isFullyCached(range)) {
      return false;
    }
    ByteBuffer target = allocate.apply(range.getLength());
    if (target == null) {
      return false;
    }
    if (copyFromCache(range.getOffset(), target) < range.getLength()) {
      return false;
    }
    target.flip();
    range.getByteBufferFuture().complete(target);
    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    telemetry.recordMetric(
        Metric.PREFETCH_BYTES_CONSUMED, range.getLength(), Collections.emptyMap());
    return true;
  }

  private boolean isFullyCached(GcsObjectRange range) {
    long endOffset = range.getOffset() + range.getLength();
    for (long blockOffset = bufferCache.alignDown(range.getOffset());
        blockOffset < endOffset;
        blockOffset += bufferCache.getBlockSizeBytes()) {
      if (!bufferCache.isCached(itemId, blockOffset)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Arranges for {@code range} to be completed once the speculative requests covering it settle,
   * and returns whether every missing block was indeed already being fetched.
   */
  private boolean completeWhenInFlightBlocksArrive(
      GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    List<CompletableFuture<ByteBuffer>> pendingBlocks = new ArrayList<>();
    long endOffset = range.getOffset() + range.getLength();
    for (long blockOffset = bufferCache.alignDown(range.getOffset());
        blockOffset < endOffset;
        blockOffset += bufferCache.getBlockSizeBytes()) {
      if (bufferCache.isCached(itemId, blockOffset)) {
        continue;
      }
      CompletableFuture<ByteBuffer> pending = pendingBlockFuture(blockOffset);
      if (pending == null) {
        return false;
      }
      pendingBlocks.add(pending);
    }
    if (pendingBlocks.isEmpty()) {
      return false;
    }
    CompletableFuture<Void> unused =
        CompletableFuture.allOf(pendingBlocks.toArray(new CompletableFuture<?>[0]))
            .whenComplete((ignored, error) -> completeAfterPrefetch(range, allocate));
    return true;
  }

  /** Serves {@code range} from the blocks that just arrived, falling back to a read of its own. */
  private void completeAfterPrefetch(GcsObjectRange range, IntFunction<ByteBuffer> allocate) {
    if (completeFromCache(range, allocate)) {
      return;
    }
    VectoredSeekableByteChannel source = sourceChannel;
    if (source == null) {
      range
          .getByteBufferFuture()
          .completeExceptionally(
              new IOException(
                  String.format("No channel available to read range %s of %s", range, itemId)));
      return;
    }
    try {
      source.readVectored(ImmutableList.of(range), allocate);
    } catch (IOException | RuntimeException e) {
      range.getByteBufferFuture().completeExceptionally(e);
    }
  }

  /**
   * Records the columns this read touched.
   *
   * <p>Recording uses the requested range rather than the block the read lands in, so that a column
   * sharing a block with columns the query ignores does not drag them along.
   */
  private void recordColumnsForRead(long position, int requestedLength) {
    int rowGroupOrdinal = findRowGroupOrdinal(position);
    if (rowGroupOrdinal < 0) {
      return;
    }
    recordColumnsInRange(rowGroupOrdinal, position, position + requestedLength);
  }

  /** Records the columns covered by {@code ranges} and notes the row group to speculate next. */
  private void recordVectoredAccess(List<GcsObjectRange> ranges) {
    int lastRowGroupOrdinal = -1;
    for (GcsObjectRange range : ranges) {
      int rowGroupOrdinal = findRowGroupOrdinal(range.getOffset());
      if (rowGroupOrdinal < 0) {
        continue;
      }
      recordColumnsInRange(
          rowGroupOrdinal, range.getOffset(), range.getOffset() + range.getLength());
      lastRowGroupOrdinal = Math.max(lastRowGroupOrdinal, rowGroupOrdinal);
    }
    if (lastRowGroupOrdinal < 0) {
      return;
    }
    pendingSpeculationRowGroupOrdinal = lastRowGroupOrdinal + 1;
  }

  /** Records every column of {@code rowGroupOrdinal} whose bytes fall in the given byte range. */
  private void recordColumnsInRange(int rowGroupOrdinal, long startOffset, long endOffset) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return;
    }
    int fingerprint = layout.getSchemaFingerprint();
    for (ParquetColumnChunk chunk : rowGroup.get().getColumnChunks().values()) {
      if (overlaps(chunk.getDataPageOffset(), chunk.getEndOffset(), startOffset, endOffset)) {
        accessHistory.recordDataAccess(fingerprint, chunk.getColumnPath());
        continue;
      }
      if (chunk
          .getDictionaryPageRange()
          .filter(
              range ->
                  overlaps(range.lowerEndpoint(), range.upperEndpoint(), startOffset, endOffset))
          .isPresent()) {
        accessHistory.recordDictionaryAccess(fingerprint, chunk.getColumnPath());
      }
    }
  }

  private static boolean overlaps(long start, long end, long otherStart, long otherEnd) {
    return start < otherEnd && end > otherStart;
  }

  /**
   * Fetches the columns already known for this schema in the first row group, once per stream.
   *
   * <p>Doing this as soon as the layout is known matters for files that are opened by reading their
   * footer: that read lands outside every row group, so without it nothing would be fetched until
   * the engine asks for column data it then has to wait for.
   */
  private void prefetchFirstRowGroupOnce(VectoredSeekableByteChannel source) {
    if (firstRowGroupPrefetched) {
      return;
    }
    firstRowGroupPrefetched = true;
    speculateRowGroup(source, 0);
  }

  /**
   * Fetches the columns already known for this schema in the current row group and the next one.
   *
   * <p>Restricting lookahead to a single row group bounds resident memory, which matters because a
   * file can hold dozens of row groups and a query may stop after the first.
   *
   * <p>Candidates up to and including {@code currentBlockOffset} are dropped: the reader has moved
   * past the blocks behind it, and the channel is already streaming the one it is sitting in.
   */
  private void speculateRowGroups(
      VectoredSeekableByteChannel source, int rowGroupOrdinal, long currentBlockOffset) {
    SortedSet<Long> blockOffsets = collectLearnedBlocks(rowGroupOrdinal, rowGroupOrdinal + 1);
    SortedSet<Long> blocksAhead =
        blockOffsets.tailSet(currentBlockOffset + bufferCache.getBlockSizeBytes());
    if (blocksAhead.isEmpty()) {
      return;
    }
    scheduler.schedule(source, itemId, blocksAhead, fileSize);
  }

  /** Fetches the columns already known for this schema in a single row group. */
  private void speculateRowGroup(VectoredSeekableByteChannel source, int rowGroupOrdinal) {
    SortedSet<Long> blockOffsets = collectLearnedBlocks(rowGroupOrdinal, rowGroupOrdinal);
    if (blockOffsets.isEmpty()) {
      return;
    }
    scheduler.schedule(source, itemId, blockOffsets, fileSize);
  }

  /**
   * Returns the block offsets covering the columns known for this schema across the given inclusive
   * range of row groups, or an empty set when nothing has been learned yet.
   */
  private SortedSet<Long> collectLearnedBlocks(int firstRowGroupOrdinal, int lastRowGroupOrdinal) {
    int fingerprint = layout.getSchemaFingerprint();
    ImmutableSet<String> dataColumns = accessHistory.getDataColumns(fingerprint);
    ImmutableSet<String> dictionaryColumns = accessHistory.getDictionaryColumns(fingerprint);
    SortedSet<Long> blockOffsets = new TreeSet<>();
    if (dataColumns.isEmpty() && dictionaryColumns.isEmpty()) {
      return blockOffsets;
    }
    for (int ordinal = firstRowGroupOrdinal; ordinal <= lastRowGroupOrdinal; ordinal++) {
      collectRowGroupBlocks(blockOffsets, ordinal, dataColumns, dictionaryColumns);
    }
    return blockOffsets;
  }

  private void collectRowGroupBlocks(
      SortedSet<Long> blockOffsets,
      int rowGroupOrdinal,
      Set<String> dataColumns,
      Set<String> dictionaryColumns) {
    Optional<ParquetRowGroup> rowGroup = layout.getRowGroup(rowGroupOrdinal);
    if (!rowGroup.isPresent()) {
      return;
    }
    for (String columnPath : dataColumns) {
      rowGroup
          .get()
          .getColumnChunk(columnPath)
          .ifPresent(
              chunk -> addBlocks(blockOffsets, chunk.getStartOffset(), chunk.getEndOffset()));
    }
    for (String columnPath : dictionaryColumns) {
      rowGroup
          .get()
          .getColumnChunk(columnPath)
          .flatMap(ParquetColumnChunk::getDictionaryPageRange)
          .ifPresent(
              range -> addBlocks(blockOffsets, range.lowerEndpoint(), range.upperEndpoint()));
    }
  }

  private void addBlocks(SortedSet<Long> blockOffsets, long startOffset, long endOffset) {
    for (long blockOffset = bufferCache.alignDown(startOffset);
        blockOffset < endOffset;
        blockOffset += bufferCache.getBlockSizeBytes()) {
      blockOffsets.add(blockOffset);
    }
  }

  /** Returns the ordinal of the row group containing {@code position}, or {@code -1} if none. */
  private int findRowGroupOrdinal(long position) {
    if (rowGroupStartOffsets.length == 0 || position < rowGroupStartOffsets[0]) {
      return -1;
    }
    int searchResult = Arrays.binarySearch(rowGroupStartOffsets, position);
    int ordinal = searchResult >= 0 ? searchResult : -searchResult - 2;
    return position < rowGroupEndOffsets[ordinal] ? ordinal : -1;
  }

  private void ensureLayoutLoaded(VectoredSeekableByteChannel source) {
    if (layoutLoadAttempted) {
      return;
    }
    layoutLoadAttempted = true;
    try {
      ByteBuffer sharedTail = cacheManager.getFooter(itemId, unused -> readTail(source));
      layout = ParquetFooterParser.parse(sharedTail, fileSize).orElse(null);
      if (layout == null && sharedTail.remaining() < desiredTailLength()) {
        layout = ParquetFooterParser.parse(readTail(source), fileSize).orElse(null);
      }
    } catch (IOException | RuntimeException e) {
      layout = null;
    }
    cacheRowGroupBoundaries();
  }

  /**
   * Snapshots the row group byte ranges once, because deriving them from the column chunks is
   * linear in the number of columns and the lookup runs on every read.
   */
  private void cacheRowGroupBoundaries() {
    if (layout == null) {
      rowGroupStartOffsets = new long[0];
      rowGroupEndOffsets = new long[0];
      return;
    }
    int rowGroupCount = layout.getRowGroups().size();
    rowGroupStartOffsets = new long[rowGroupCount];
    rowGroupEndOffsets = new long[rowGroupCount];
    for (int ordinal = 0; ordinal < rowGroupCount; ordinal++) {
      ParquetRowGroup rowGroup = layout.getRowGroups().get(ordinal);
      rowGroupStartOffsets[ordinal] = rowGroup.getStartOffset();
      rowGroupEndOffsets[ordinal] = rowGroup.getEndOffset();
    }
  }

  private int desiredTailLength() {
    return (int) Math.min(fileSize, FOOTER_READ_BYTES);
  }

  private ByteBuffer readTail(VectoredSeekableByteChannel source) throws IOException {
    int tailLength = desiredTailLength();
    ByteBuffer tail = ByteBuffer.allocate(tailLength);
    long originalPosition = source.position();
    try {
      source.position(fileSize - tailLength);
      while (tail.hasRemaining() && source.read(tail) != -1) {
        // Reading until the tail is filled or the object ends.
      }
      tail.flip();
      return tail;
    } finally {
      source.position(originalPosition);
    }
  }
}
