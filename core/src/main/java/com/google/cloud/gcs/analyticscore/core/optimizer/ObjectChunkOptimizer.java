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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectChunkKey;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

/**
 * A {@link FormatOptimizer} that caches object data in fixed-size, chunk-aligned blocks and serves
 * {@code read} requests from that cache.
 *
 * <p>On a read it partitions the requested range into chunks; each chunk is served from the cache
 * (a hit) or downloaded and cached (a miss). To avoid issuing one GCS request per missing chunk,
 * each maximal run of consecutive missing chunks is fetched with a single request, then split into
 * chunk-sized blocks that are cached individually. If the missing chunks are fragmented across more
 * than {@code objectChunkMaxFetchSplits} runs, the whole covering range is fetched with a single
 * request instead, bounding the number of requests per read. Because GCS already fetches at least a
 * minimum request size for random reads, caching whole aligned chunks lets repeated or nearby reads
 * be served from memory instead of re-downloaded.
 *
 * <p>It only activates when the file size is known (the channel was opened with file metadata) and
 * never downloads or serves bytes beyond the end of the object. Vectored reads apply the same
 * per-chunk logic to each requested range (serving cached chunks and loading/caching missing ones),
 * so chunks populated by either path are reused by both.
 */
public class ObjectChunkOptimizer implements FormatOptimizer {

  private final GcsCacheOptions cacheOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId itemId;
  private long generation = -1;
  private long fileSize = -1;
  private int chunkSize;
  private int maxFetchSplits;

  public ObjectChunkOptimizer(GcsCacheOptions cacheOptions, Telemetry telemetry) {
    this.cacheOptions = checkNotNull(cacheOptions, "cacheOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    // File size is required (and only available via file metadata) to bound reads to the object.
    return false;
  }

  @Override
  public boolean isApplicable(GcsFileInfo fileInfo) {
    return cacheOptions.isObjectChunkCacheEnabled() && fileInfo.getItemInfo().getSize() >= 0;
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.itemId = itemId;
    this.cacheManager = cacheManager;
    this.chunkSize = cacheOptions.getObjectChunkSizeBytes();
    this.maxFetchSplits = cacheOptions.getObjectChunkMaxFetchSplits();
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.itemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
    this.generation = fileInfo.getItemInfo().getContentGeneration().orElse(-1L);
    this.chunkSize = cacheOptions.getObjectChunkSizeBytes();
    this.maxFetchSplits = cacheOptions.getObjectChunkMaxFetchSplits();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (!isActive()) {
      return 0;
    }
    if (position >= fileSize) {
      return -1;
    }
    long originalPosition = source.position();
    try {
      return fillFromChunks(position, dst, source);
    } finally {
      // Chunk loads move the shared cursor; restore it so the channel stays consistent.
      source.position(originalPosition);
    }
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source)
      throws IOException {
    if (!isActive()) {
      return ranges;
    }
    long originalPosition = source.position();
    try {
      List<GcsObjectRange> deferred = new ArrayList<>();
      for (GcsObjectRange range : ranges) {
        serveRange(range, allocate, source, deferred);
      }
      return deferred;
    } finally {
      // Chunk loads move the shared cursor; restore it so the channel stays consistent.
      source.position(originalPosition);
    }
  }

  /**
   * Serves one requested range with the same chunk logic as {@link #read}: it fills the range from
   * cached chunks, loading and caching any missing chunks from {@code source}, and completes the
   * range's future. Ranges that fall outside the object are deferred to the underlying channel.
   */
  private void serveRange(
      GcsObjectRange range,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source,
      List<GcsObjectRange> deferred) {
    long offset = range.getOffset();
    int length = range.getLength();
    if (offset < 0 || length < 0 || offset + (long) length > fileSize) {
      deferred.add(range);
      return;
    }
    CompletableFuture<ByteBuffer> future = range.getByteBufferFuture();
    try {
      ByteBuffer dest = allocate.apply(length);
      if (dest == null) {
        future.completeExceptionally(
            new IllegalArgumentException(
                String.format("Buffer allocation returned null for range: %s", range)));
        return;
      }
      int served = fillFromChunks(offset, dest, source);
      if (served < length) {
        future.completeExceptionally(
            new EOFException(
                String.format("Error while populating range: %s, unexpected EOF", range)));
        return;
      }
      dest.flip();
      future.complete(dest);
    } catch (IOException | RuntimeException e) {
      future.completeExceptionally(e);
    }
  }

  /**
   * Copies the requested read into {@code dst} by resolving every chunk it touches (from cache or
   * by fetching) and copying from those in-memory buffers. Returns the number of bytes copied,
   * bounded by the space in {@code dst} and the end of the object.
   */
  private int fillFromChunks(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    long readEnd = Math.min(position + dst.remaining(), fileSize);
    if (position >= readEnd) {
      return 0;
    }
    long firstChunkIndex = position / chunkSize;
    long lastChunkIndex = (readEnd - 1) / chunkSize;
    Map<Long, ByteBuffer> chunks = resolveChunks(firstChunkIndex, lastChunkIndex, source);
    return copyChunksInto(dst, position, chunks);
  }

  /**
   * Returns a buffer for every chunk in {@code [firstChunkIndex, lastChunkIndex]}: cached chunks
   * are served from the cache, missing chunks are fetched from {@code source} and cached. Each
   * returned buffer is an independent view the caller may freely reposition.
   */
  private Map<Long, ByteBuffer> resolveChunks(
      long firstChunkIndex, long lastChunkIndex, VectoredSeekableByteChannel source)
      throws IOException {
    Map<Long, ByteBuffer> chunks = new HashMap<>();
    List<ChunkRun> missingRuns = probeCache(firstChunkIndex, lastChunkIndex, chunks);
    fetchMissingChunks(missingRuns, firstChunkIndex, lastChunkIndex, source, chunks);
    return chunks;
  }

  /**
   * Looks up every chunk in {@code [firstChunkIndex, lastChunkIndex]}, adding the cached ones to
   * {@code chunks} (and recording hit/miss metrics), and returns the runs of consecutive indices
   * that were not cached.
   */
  private List<ChunkRun> probeCache(
      long firstChunkIndex, long lastChunkIndex, Map<Long, ByteBuffer> chunks) {
    List<ChunkRun> missingRuns = new ArrayList<>();
    long runStart = -1;
    for (long chunkIndex = firstChunkIndex; chunkIndex <= lastChunkIndex; chunkIndex++) {
      Optional<ByteBuffer> cached = cacheManager.getObjectChunkIfPresent(chunkKey(chunkIndex));
      if (!cached.isPresent()) {
        recordCacheMiss();
        if (runStart == -1) {
          runStart = chunkIndex;
        }
        continue;
      }
      recordCacheHit();
      chunks.put(chunkIndex, cached.get());
      if (runStart != -1) {
        missingRuns.add(new ChunkRun(runStart, chunkIndex - 1));
        runStart = -1;
      }
    }
    if (runStart != -1) {
      missingRuns.add(new ChunkRun(runStart, lastChunkIndex));
    }
    return missingRuns;
  }

  /**
   * Fetches every missing chunk into {@code chunks}. Each run is normally fetched with its own
   * request, but if the misses are fragmented across more than {@code maxFetchSplits} runs the
   * whole covering range is fetched with a single request instead (re-reading the cached gaps), so
   * a fragmented cache can never explode into many small requests.
   */
  private void fetchMissingChunks(
      List<ChunkRun> missingRuns,
      long firstChunkIndex,
      long lastChunkIndex,
      VectoredSeekableByteChannel source,
      Map<Long, ByteBuffer> chunks)
      throws IOException {
    if (missingRuns.isEmpty()) {
      return;
    }
    if (missingRuns.size() > maxFetchSplits) {
      fetchRun(new ChunkRun(firstChunkIndex, lastChunkIndex), source, chunks);
      return;
    }
    for (ChunkRun run : missingRuns) {
      fetchRun(run, source, chunks);
    }
  }

  /**
   * Downloads the chunks in {@code run} with a single request, then splits the bytes into per-chunk
   * buffers, caching each one and adding it to {@code chunks}.
   */
  private void fetchRun(
      ChunkRun run, VectoredSeekableByteChannel source, Map<Long, ByteBuffer> chunks)
      throws IOException {
    long rangeStart = chunkStart(run.first);
    long rangeEnd = chunkEnd(run.last);
    ByteBuffer data = readRange(source, rangeStart, (int) (rangeEnd - rangeStart));
    for (long chunkIndex = run.first; chunkIndex <= run.last; chunkIndex++) {
      ByteBuffer chunk = copyChunk(data, rangeStart, chunkIndex);
      cacheManager.putObjectChunk(chunkKey(chunkIndex), chunk);
      chunks.put(chunkIndex, chunk.asReadOnlyBuffer());
    }
  }

  /**
   * Copies the bytes for {@code chunkIndex} out of {@code data} (which begins at byte {@code
   * rangeStart}) into its own backing array, so each cached chunk can be evicted independently.
   */
  private ByteBuffer copyChunk(ByteBuffer data, long rangeStart, long chunkIndex) {
    int length = (int) (chunkEnd(chunkIndex) - chunkStart(chunkIndex));
    ByteBuffer slice = data.duplicate();
    slice.position((int) (chunkStart(chunkIndex) - rangeStart));
    slice.limit(slice.position() + length);

    ByteBuffer chunk = ByteBuffer.allocate(length);
    chunk.put(slice);
    chunk.flip();
    return chunk;
  }

  /**
   * Copies bytes from the resolved {@code chunks} into {@code dst}, starting at {@code position}
   * and stopping at the end of {@code dst} or of the object. Returns the number of bytes copied.
   */
  private int copyChunksInto(ByteBuffer dst, long position, Map<Long, ByteBuffer> chunks) {
    int total = 0;
    while (dst.hasRemaining() && position < fileSize) {
      long chunkIndex = position / chunkSize;
      int offsetInChunk = (int) (position - chunkStart(chunkIndex));
      int bytesToCopy = Math.min(dst.remaining(), (int) (chunkEnd(chunkIndex) - position));

      // Each map entry is a private read-only view, so positioning it here is safe.
      ByteBuffer chunk = chunks.get(chunkIndex);
      chunk.position(offsetInChunk);
      chunk.limit(offsetInChunk + bytesToCopy);
      dst.put(chunk);

      position += bytesToCopy;
      total += bytesToCopy;
    }
    return total;
  }

  /** Reads exactly {@code length} bytes starting at {@code start} into a fresh buffer. */
  private ByteBuffer readRange(VectoredSeekableByteChannel source, long start, int length)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    source.position(start);
    while (buffer.hasRemaining()) {
      if (source.read(buffer) == -1) {
        throw new IOException(
            String.format(
                "Unexpected EOF while reading range at offset %d (length %d) of %s",
                start, length, itemId));
      }
    }
    buffer.flip();
    return buffer;
  }

  private void recordCacheHit() {
    telemetry.recordMetric(Metric.OBJECT_CHUNK_CACHE_HIT, 1L, Collections.emptyMap());
  }

  private void recordCacheMiss() {
    telemetry.recordMetric(Metric.OBJECT_CHUNK_CACHE_MISS, 1L, Collections.emptyMap());
  }

  /** Returns the byte offset where {@code chunkIndex} begins. */
  private long chunkStart(long chunkIndex) {
    return chunkIndex * (long) chunkSize;
  }

  /** Returns the byte offset where {@code chunkIndex} ends, bounded by the end of the object. */
  private long chunkEnd(long chunkIndex) {
    return Math.min(chunkStart(chunkIndex) + chunkSize, fileSize);
  }

  private boolean isActive() {
    return cacheOptions.isObjectChunkCacheEnabled() && fileSize >= 0 && chunkSize > 0;
  }

  private GcsObjectChunkKey chunkKey(long chunkIndex) {
    return GcsObjectChunkKey.builder()
        .setItemId(itemId)
        .setGeneration(generation)
        .setChunkIndex(chunkIndex)
        .build();
  }

  /** An inclusive range of consecutive chunk indices. */
  private static final class ChunkRun {
    final long first;
    final long last;

    ChunkRun(long first, long last) {
      this.first = first;
      this.last = last;
    }
  }
}
