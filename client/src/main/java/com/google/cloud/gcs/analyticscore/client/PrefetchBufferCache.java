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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * Holds speculatively fetched bytes as exact byte ranges addressed by absolute file offset, and
 * serves single-buffer and vectored reads from those ranges.
 *
 * <p>Each cached entry is encapsulated in a {@link CachedRange} value object holding a {@link
 * CompletableFuture}, unifying in-flight background downloads and resident buffers under a single
 * representation.
 *
 * <p>A small range may be cached alongside a larger range that contains it, for example a
 * dictionary page next to the whole column chunk. A read is served by the smallest range covering
 * it, so it never waits on a larger download it does not need, and an exact match is served without
 * copying. Each range is evicted once every one of its bytes has been read.
 *
 * <p>Entries are bounded by total bytes and expire once they have been idle for the configured
 * time.
 *
 * <p>This class is thread-safe.
 */
public final class PrefetchBufferCache {

  private static final Comparator<RangeKey> BY_START_THEN_END =
      Comparator.comparingLong(RangeKey::getStartOffset).thenComparingLong(RangeKey::getEndOffset);

  private final Cache<RangeKey, CachedRange> ranges;
  private final Telemetry telemetry;
  private final ConcurrentHashMap<GcsItemId, ConcurrentSkipListSet<RangeKey>> rangeIndexByItem =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<GcsItemId, Integer> openStreamsByItem = new ConcurrentHashMap<>();
  private final AtomicInteger maxRangeLength = new AtomicInteger();

  /**
   * Creates a cache storing exact byte ranges.
   *
   * @param maxSizeBytes the maximum total size of retained buffers
   * @param ttlSeconds how long a range is retained after it was last read or written
   * @param telemetry telemetry recorder for prefetch cache metrics
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds, Telemetry telemetry) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(ttlSeconds > 0, "ttlSeconds must be positive");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
    this.ranges =
        Caffeine.newBuilder()
            .maximumWeight(maxSizeBytes)
            .weigher((RangeKey key, CachedRange value) -> value.getLength())
            .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
            .removalListener(
                (RangeKey key, CachedRange value, RemovalCause cause) -> {
                  if (key != null && cause != RemovalCause.REPLACED) {
                    forgetKey(key);
                  }
                })
            .build();
  }

  /**
   * Registers an in-flight or completed range {@code [startOffset, startOffset + length)} in the
   * cache if no existing entry already covers it.
   *
   * @return {@code true} if the range was newly registered, or {@code false} if an existing entry
   *     (resident or in-flight) already covers the range
   */
  public boolean registerRange(
      GcsItemId itemId, long startOffset, int length, CompletableFuture<ByteBuffer> future) {
    return registerRange(itemId, startOffset, length, future, () -> {});
  }

  /**
   * Registers an in-flight or completed range {@code [startOffset, startOffset + length)} with a
   * callback that promotes the background download to foreground priority if demanded.
   */
  public boolean registerRange(
      GcsItemId itemId,
      long startOffset,
      int length,
      CompletableFuture<ByteBuffer> future,
      Runnable promotionAction) {
    checkNotNull(itemId, "itemId cannot be null");
    checkArgument(length > 0, "length %s must be positive", length);
    CachedRange cachedRange =
        CachedRange.create(startOffset, startOffset + length, future, promotionAction);
    if (!tryRegisterInIndex(itemId, cachedRange)) {
      return false;
    }
    CompletableFuture<ByteBuffer> unused =
        future.whenComplete(
            (buffer, error) -> {
              if (error != null || buffer == null) {
                removeRange(itemId, cachedRange);
              }
            });
    return true;
  }

  private boolean tryRegisterInIndex(GcsItemId itemId, CachedRange cachedRange) {
    AtomicBoolean registered = new AtomicBoolean();
    rangeIndexByItem.compute(
        itemId,
        (id, existingKeys) -> {
          ConcurrentSkipListSet<RangeKey> itemKeys =
              existingKeys != null ? existingKeys : new ConcurrentSkipListSet<>(BY_START_THEN_END);
          long startOffset = cachedRange.getStartOffset();
          if (findSmallestCovering(id, itemKeys, startOffset, cachedRange.getLength())
              .isPresent()) {
            return itemKeys.isEmpty() ? null : itemKeys;
          }
          RangeKey key = RangeKey.create(id, startOffset, cachedRange.getEndOffset());
          maxRangeLength.accumulateAndGet(cachedRange.getLength(), Math::max);
          ranges.put(key, cachedRange);
          itemKeys.add(key);
          registered.set(true);
          return itemKeys;
        });
    return registered.get();
  }

  /**
   * Returns the smallest {@link CachedRange} (in-flight or resident) that completely covers {@code
   * [offset, offset + length)}, or {@code Optional.empty()} if no single cached range covers it.
   */
  public Optional<CachedRange> getRangeCovering(GcsItemId itemId, long offset, int length) {
    checkNotNull(itemId, "itemId cannot be null");
    return findSmallestCovering(itemId, rangeIndexByItem.get(itemId), offset, length);
  }

  /**
   * Copies cached bytes for {@code itemId} starting at {@code position} into {@code dst}, records
   * cache hit or miss telemetry, and returns the number of bytes served (or {@code 0} on a miss).
   */
  public int serveFromCache(GcsItemId itemId, long position, ByteBuffer dst, boolean recordMiss) {
    int servedBytes = copyInto(itemId, position, dst);
    if (servedBytes == 0) {
      recordCacheMiss(recordMiss);
      return 0;
    }
    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
    return servedBytes;
  }

  /**
   * Completes any ranges in {@code ranges} that are cached or in-flight for {@code itemId}, falling
   * back to {@code source} if an in-flight range fails, and returns the remaining unserved ranges.
   */
  public ImmutableList<GcsObjectRange> serveVectoredFromCache(
      GcsItemId itemId,
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source,
      boolean recordMiss) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(ranges, "ranges cannot be null");
    checkNotNull(allocate, "allocate cannot be null");
    checkNotNull(source, "source cannot be null");
    ImmutableList.Builder<GcsObjectRange> unservedRanges = ImmutableList.builder();
    for (GcsObjectRange range : ranges) {
      if (!tryCompleteFromCache(itemId, range, allocate, source, recordMiss)) {
        unservedRanges.add(range);
      }
    }
    return unservedRanges.build();
  }

  /**
   * Returns a future holding the cached bytes covering {@code [offset, offset + length)}, or {@code
   * Optional.empty()} if the window is not completely covered. A window matching one cached range
   * exactly reuses its buffer; otherwise the bytes are copied once into a buffer from {@code
   * allocate}, stitching contiguous ranges if needed. Any covered range is evicted once all of its
   * bytes have been consumed.
   */
  Optional<CompletableFuture<ByteBuffer>> copyIntoAsync(
      GcsItemId itemId, long offset, int length, IntFunction<ByteBuffer> allocate) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(allocate, "allocate cannot be null");
    Optional<ImmutableList<CachedRange>> segments = collectContiguousRanges(itemId, offset, length);
    if (!segments.isPresent()) {
      return Optional.empty();
    }
    ImmutableList<CachedRange> coveredSegments = segments.get();
    consumeSegments(itemId, coveredSegments, offset, offset + length);
    if (coveredSegments.size() == 1) {
      return Optional.of(coveredSegments.get(0).copyIntoAsync(offset, length, allocate));
    }
    return Optional.of(stitchSegmentsAsync(coveredSegments, offset, length, allocate));
  }

  private Optional<ImmutableList<CachedRange>> collectContiguousRanges(
      GcsItemId itemId, long offset, int length) {
    ConcurrentSkipListSet<RangeKey> itemKeys = rangeIndexByItem.get(itemId);
    if (itemKeys == null || length <= 0) {
      return Optional.empty();
    }
    ImmutableList.Builder<CachedRange> segments = ImmutableList.builder();
    long cursor = offset;
    long targetEnd = offset + length;
    while (cursor < targetEnd) {
      Optional<CachedRange> segment =
          selectRange(itemId, itemKeys, cursor, (int) (targetEnd - cursor));
      if (!segment.isPresent()) {
        return Optional.empty();
      }
      segments.add(segment.get());
      cursor = segment.get().getEndOffset();
    }
    return Optional.of(segments.build());
  }

  private void consumeSegments(
      GcsItemId itemId, ImmutableList<CachedRange> segments, long startOffset, long endOffset) {
    long cursor = startOffset;
    for (CachedRange segment : segments) {
      int sliceLength = (int) (Math.min(segment.getEndOffset(), endOffset) - cursor);
      recordBytesServed(segment, sliceLength);
      if (segment.recordBytesConsumed(cursor, sliceLength)) {
        removeRange(itemId, segment);
      }
      cursor += sliceLength;
    }
  }

  private void recordBytesServed(CachedRange range, int bytes) {
    int newlyServed = range.recordBytesServed(bytes);
    if (newlyServed > 0) {
      telemetry.recordMetric(Metric.PREFETCH_BYTES_CONSUMED, newlyServed, Collections.emptyMap());
    }
  }

  private static CompletableFuture<ByteBuffer> stitchSegmentsAsync(
      ImmutableList<CachedRange> segments,
      long offset,
      int length,
      IntFunction<ByteBuffer> allocate) {
    long targetEnd = offset + length;
    List<CompletableFuture<ByteBuffer>> sliceFutures = new ArrayList<>(segments.size());
    long cursor = offset;
    for (CachedRange segment : segments) {
      int sliceLength = (int) (Math.min(segment.getEndOffset(), targetEnd) - cursor);
      sliceFutures.add(segment.sliceAsync(cursor, sliceLength));
      cursor += sliceLength;
    }
    return CompletableFuture.allOf(sliceFutures.toArray(new CompletableFuture<?>[0]))
        .thenApply(
            unused -> {
              ByteBuffer target = allocate.apply(length);
              for (CompletableFuture<ByteBuffer> sliceFuture : sliceFutures) {
                target.put(sliceFuture.join());
              }
              target.flip();
              return target;
            });
  }

  /**
   * Copies bytes covering {@code position} into {@code dst} (waiting if the covering range is
   * currently in flight) and returns the number of bytes copied, or {@code 0} if no cached range
   * covers {@code position}. A range is evicted once all of its bytes have been consumed.
   */
  int copyInto(GcsItemId itemId, long position, ByteBuffer dst) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(dst, "dst cannot be null");
    int totalCopied = 0;
    while (dst.hasRemaining()) {
      long currentPos = position + totalCopied;
      Optional<CachedRange> selected =
          selectRange(itemId, rangeIndexByItem.get(itemId), currentPos, dst.remaining());
      if (!selected.isPresent()) {
        break;
      }
      CachedRange range = selected.get();
      int copied = range.copyInto(currentPos, dst);
      if (copied == 0) {
        break;
      }
      recordBytesServed(range, copied);
      if (range.recordBytesConsumed(currentPos, copied)) {
        removeRange(itemId, range);
      }
      totalCopied += copied;
    }
    return totalCopied;
  }

  private boolean tryCompleteFromCache(
      GcsItemId itemId,
      GcsObjectRange range,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source,
      boolean recordMiss) {
    Optional<CompletableFuture<ByteBuffer>> populatedFuture =
        copyIntoAsync(itemId, range.getOffset(), range.getLength(), allocate);
    if (!populatedFuture.isPresent()) {
      recordCacheMiss(recordMiss);
      return false;
    }
    CompletableFuture<ByteBuffer> unused =
        populatedFuture
            .get()
            .whenComplete(
                (target, error) -> {
                  if (error == null) {
                    range.getByteBufferFuture().complete(target);
                    telemetry.recordMetric(Metric.PREFETCH_CACHE_HIT, 1L, Collections.emptyMap());
                    return;
                  }
                  recordCacheMiss(recordMiss);
                  fallbackToSourceRead(range, allocate, source);
                });
    return true;
  }

  private static void fallbackToSourceRead(
      GcsObjectRange range, IntFunction<ByteBuffer> allocate, VectoredSeekableByteChannel source) {
    try {
      source.readVectored(ImmutableList.of(range), allocate);
    } catch (IOException | RuntimeException e) {
      range.getByteBufferFuture().completeExceptionally(e);
    }
  }

  private void recordCacheMiss(boolean recordMiss) {
    if (recordMiss) {
      telemetry.recordMetric(Metric.PREFETCH_CACHE_MISS, 1L, Collections.emptyMap());
    }
  }

  /**
   * Registers an open stream reading {@code itemId} and returns a handle that evicts the item's
   * cached ranges when the last open stream closes.
   */
  public RegisteredStream registerStream(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    openStreamsByItem.merge(itemId, 1, Integer::sum);
    return new RegisteredStream(() -> releaseStream(itemId));
  }

  private void releaseStream(GcsItemId itemId) {
    openStreamsByItem.computeIfPresent(
        itemId,
        (id, count) -> {
          if (count > 1) {
            return count - 1;
          }
          evictAllForItem(id);
          return null;
        });
  }

  private void evictAllForItem(GcsItemId itemId) {
    ConcurrentSkipListSet<RangeKey> itemKeys = rangeIndexByItem.remove(itemId);
    if (itemKeys == null) {
      return;
    }
    for (RangeKey key : itemKeys) {
      ranges.invalidate(key);
    }
  }

  /**
   * Evicts the cached range {@code [startOffset, endOffset)} of {@code itemId} if it is still
   * backed by {@code future}, so a stream only evicts the entries it registered.
   */
  public void evictRange(
      GcsItemId itemId, long startOffset, long endOffset, CompletableFuture<ByteBuffer> future) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(future, "future cannot be null");
    CachedRange cached = ranges.getIfPresent(RangeKey.create(itemId, startOffset, endOffset));
    if (cached != null && cached.getFuture() == future) {
      removeRange(itemId, cached);
    }
  }

  /** Discards all cached ranges. */
  public void invalidateAll() {
    ranges.invalidateAll();
    rangeIndexByItem.clear();
  }

  /**
   * Returns the smallest range covering {@code [offset, offset + length)}, or else the range
   * covering {@code offset} that reaches furthest, so a stitched read uses as few segments as
   * possible.
   */
  private Optional<CachedRange> selectRange(
      GcsItemId itemId, @Nullable NavigableSet<RangeKey> itemKeys, long offset, int length) {
    Optional<CachedRange> smallestCovering = findSmallestCovering(itemId, itemKeys, offset, length);
    if (smallestCovering.isPresent()) {
      return smallestCovering;
    }
    return findFurthestReaching(itemId, itemKeys, offset);
  }

  private Optional<CachedRange> findSmallestCovering(
      GcsItemId itemId, @Nullable NavigableSet<RangeKey> itemKeys, long offset, int length) {
    if (itemKeys == null || length < 0) {
      return Optional.empty();
    }
    long endOffset = offset + length;
    CachedRange smallest = null;
    for (RangeKey key : candidateKeys(itemId, itemKeys, offset, endOffset)) {
      boolean smaller = smallest == null || key.getLength() < smallest.getLength();
      if (key.getEndOffset() >= endOffset && smaller) {
        CachedRange cached = ranges.getIfPresent(key);
        smallest = cached != null ? cached : smallest;
      }
    }
    return Optional.ofNullable(smallest);
  }

  private Optional<CachedRange> findFurthestReaching(
      GcsItemId itemId, @Nullable NavigableSet<RangeKey> itemKeys, long offset) {
    if (itemKeys == null) {
      return Optional.empty();
    }
    CachedRange furthest = null;
    for (RangeKey key : candidateKeys(itemId, itemKeys, offset, offset + 1)) {
      boolean further = furthest == null || key.getEndOffset() > furthest.getEndOffset();
      if (key.getEndOffset() > offset && further) {
        CachedRange cached = ranges.getIfPresent(key);
        furthest = cached != null ? cached : furthest;
      }
    }
    return Optional.ofNullable(furthest);
  }

  /**
   * Returns the keys starting at or before {@code offset} that are long enough to reach {@code
   * endOffset}, nearest first. No range is longer than {@link #maxRangeLength}, which bounds the
   * scan.
   */
  private List<RangeKey> candidateKeys(
      GcsItemId itemId, NavigableSet<RangeKey> itemKeys, long offset, long endOffset) {
    long earliestStart = endOffset - maxRangeLength.get();
    List<RangeKey> candidates = new ArrayList<>();
    for (RangeKey key :
        itemKeys.headSet(RangeKey.create(itemId, offset, Long.MAX_VALUE), true).descendingSet()) {
      if (key.getStartOffset() < earliestStart) {
        break;
      }
      candidates.add(key);
    }
    return candidates;
  }

  private void removeRange(GcsItemId itemId, CachedRange cachedRange) {
    RangeKey key =
        RangeKey.create(itemId, cachedRange.getStartOffset(), cachedRange.getEndOffset());
    ranges.asMap().remove(key, cachedRange);
    forgetKey(key);
  }

  /**
   * Drops {@code key} from the index of its item when no range remains at that key, and removes the
   * item's entry once it holds no keys so that closed objects do not leak map entries.
   */
  private void forgetKey(RangeKey key) {
    rangeIndexByItem.computeIfPresent(
        key.getItemId(),
        (id, itemKeys) -> {
          if (ranges.getIfPresent(key) == null) {
            itemKeys.remove(key);
          }
          return itemKeys.isEmpty() ? null : itemKeys;
        });
  }

  /** A handle for an open stream reading an item, releasing its hold on {@link #close()}. */
  public static final class RegisteredStream implements AutoCloseable {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Runnable onClose;

    private RegisteredStream(Runnable onClose) {
      this.onClose = onClose;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        onClose.run();
      }
    }
  }

  /** Identifies a cached range by object, start offset, and end offset. */
  @AutoValue
  abstract static class RangeKey {

    abstract GcsItemId getItemId();

    abstract long getStartOffset();

    abstract long getEndOffset();

    final long getLength() {
      return getEndOffset() - getStartOffset();
    }

    static RangeKey create(GcsItemId itemId, long startOffset, long endOffset) {
      return new AutoValue_PrefetchBufferCache_RangeKey(itemId, startOffset, endOffset);
    }
  }
}
