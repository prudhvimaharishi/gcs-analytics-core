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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Holds speculatively fetched bytes as exact byte ranges addressed by absolute file offset.
 *
 * <p>Each cached entry is encapsulated in a {@link CachedRange} value object holding a {@link
 * CompletableFuture}, unifying in-flight background downloads and resident buffers under a single
 * representation.
 *
 * <p>Entries are bounded by total bytes and expire once they have been idle for the configured
 * time.
 *
 * <p>This class is thread-safe.
 */
public final class PrefetchBufferCache {

  private final Cache<RangeKey, CachedRange> ranges;
  private final ConcurrentHashMap<GcsItemId, ConcurrentSkipListSet<Long>> offsetsByItem =
      new ConcurrentHashMap<>();

  /**
   * Creates a cache storing exact byte ranges.
   *
   * @param maxSizeBytes the maximum total size of retained buffers
   * @param ttlSeconds how long a range is retained after it was last read or written
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(ttlSeconds > 0, "ttlSeconds must be positive");
    this.ranges =
        Caffeine.newBuilder()
            .maximumWeight(maxSizeBytes)
            .weigher((RangeKey key, CachedRange value) -> value.getLength())
            .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
            .removalListener(
                (RangeKey key, CachedRange value, RemovalCause cause) -> {
                  if (key != null && cause != RemovalCause.REPLACED) {
                    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(key.getItemId());
                    if (itemOffsets != null) {
                      itemOffsets.remove(key.getStartOffset());
                    }
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
    checkNotNull(future, "future cannot be null");
    checkNotNull(promotionAction, "promotionAction cannot be null");
    checkArgument(startOffset >= 0, "startOffset %s must be non-negative", startOffset);
    checkArgument(length > 0, "length %s must be positive", length);
    ConcurrentSkipListSet<Long> itemOffsets =
        offsetsByItem.computeIfAbsent(itemId, unused -> new ConcurrentSkipListSet<>());
    synchronized (itemOffsets) {
      if (getRangeCovering(itemId, startOffset, length).isPresent()) {
        return false;
      }
      long endOffset = startOffset + length;
      removeShadowedSubRangesLocked(itemId, itemOffsets, startOffset, endOffset);
      CachedRange cachedRange = CachedRange.create(startOffset, endOffset, future, promotionAction);
      RangeKey key = RangeKey.create(itemId, startOffset);
      itemOffsets.add(startOffset);
      ranges.put(key, cachedRange);
      CompletableFuture<ByteBuffer> unused =
          future.whenComplete(
              (buffer, error) -> {
                if (error != null || buffer == null) {
                  removeRange(itemId, startOffset);
                }
              });
      return true;
    }
  }

  /** Stores an already-downloaded byte buffer starting at {@code rangeStart}. */
  public void putRange(GcsItemId itemId, long rangeStart, ByteBuffer data) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(data, "data cannot be null");
    checkArgument(rangeStart >= 0, "rangeStart %s must be non-negative", rangeStart);
    if (!data.hasRemaining()) {
      return;
    }
    ByteBuffer readOnly = data.asReadOnlyBuffer();
    int length = readOnly.remaining();
    ConcurrentSkipListSet<Long> itemOffsets =
        offsetsByItem.computeIfAbsent(itemId, unused -> new ConcurrentSkipListSet<>());
    synchronized (itemOffsets) {
      long endOffset = rangeStart + length;
      removeShadowedSubRangesLocked(itemId, itemOffsets, rangeStart, endOffset);
      CachedRange cachedRange =
          CachedRange.create(rangeStart, endOffset, CompletableFuture.completedFuture(readOnly));
      itemOffsets.add(rangeStart);
      ranges.put(RangeKey.create(itemId, rangeStart), cachedRange);
    }
  }

  private void removeShadowedSubRangesLocked(
      GcsItemId itemId, ConcurrentSkipListSet<Long> itemOffsets, long startOffset, long endOffset) {
    for (Long subStart :
        new ArrayList<>(itemOffsets.subSet(startOffset, false, endOffset, false))) {
      CachedRange existing = ranges.getIfPresent(RangeKey.create(itemId, subStart));
      if (existing == null || existing.getEndOffset() <= endOffset) {
        removeRange(itemId, subStart);
      }
    }
  }

  /**
   * Returns the {@link CachedRange} (in-flight or resident) that completely covers {@code [offset,
   * offset + length)}, stitching contiguous cached ranges when necessary, or {@code
   * Optional.empty()} if any byte in the requested window is missing.
   */
  public Optional<CachedRange> getRangeCovering(GcsItemId itemId, long offset, int length) {
    checkNotNull(itemId, "itemId cannot be null");
    if (length < 0) {
      return Optional.empty();
    }
    Optional<CachedRange> firstRange = findRange(itemId, offset);
    if (!firstRange.isPresent()) {
      return Optional.empty();
    }
    if (firstRange.get().contains(offset, length)) {
      return firstRange;
    }
    long targetEnd = offset + length;
    if (offset < firstRange.get().getStartOffset() || offset >= firstRange.get().getEndOffset()) {
      return Optional.empty();
    }
    return collectContiguousRanges(itemId, firstRange.get(), targetEnd)
        .map(segments -> stitchRanges(segments, offset, length));
  }

  private Optional<List<CachedRange>> collectContiguousRanges(
      GcsItemId itemId, CachedRange firstRange, long targetEnd) {
    List<CachedRange> segments = new ArrayList<>();
    segments.add(firstRange);
    long cursor = firstRange.getEndOffset();
    while (cursor < targetEnd) {
      Optional<CachedRange> nextRange = findRange(itemId, cursor);
      if (!nextRange.isPresent()
          || nextRange.get().getStartOffset() > cursor
          || nextRange.get().getEndOffset() <= cursor) {
        return Optional.empty();
      }
      segments.add(nextRange.get());
      cursor = nextRange.get().getEndOffset();
    }
    return Optional.of(segments);
  }

  private static CachedRange stitchRanges(List<CachedRange> segments, long offset, int length) {
    long targetEnd = offset + length;
    List<CompletableFuture<ByteBuffer>> sliceFutures = new ArrayList<>(segments.size());
    long cursor = offset;
    for (CachedRange segment : segments) {
      if (cursor >= targetEnd) {
        break;
      }
      int sliceLength = (int) (Math.min(segment.getEndOffset(), targetEnd) - cursor);
      sliceFutures.add(segment.slice(cursor, sliceLength));
      cursor += sliceLength;
    }
    CompletableFuture<ByteBuffer> combinedFuture =
        CompletableFuture.allOf(sliceFutures.toArray(new CompletableFuture<?>[0]))
            .thenApply(
                unused -> {
                  ByteBuffer combined = ByteBuffer.allocate(length);
                  for (CompletableFuture<ByteBuffer> sliceFuture : sliceFutures) {
                    combined.put(sliceFuture.join());
                  }
                  combined.flip();
                  return combined.asReadOnlyBuffer();
                });
    return CachedRange.create(offset, targetEnd, combinedFuture);
  }

  /**
   * Consumes the cached bytes covering {@code [offset, offset + length)}, evicting any cached
   * segments whose end offset falls within the consumed window, and returns a future that allocates
   * the target buffer once via {@code allocate} and copies the segment slices directly into it.
   */
  public Optional<CompletableFuture<ByteBuffer>> consumeRange(
      GcsItemId itemId, long offset, int length, IntFunction<ByteBuffer> allocate) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(allocate, "allocate cannot be null");
    if (length <= 0) {
      return Optional.empty();
    }
    Optional<CachedRange> firstRange = findRange(itemId, offset);
    if (!firstRange.isPresent()) {
      return Optional.empty();
    }
    long targetEnd = offset + length;
    if (offset < firstRange.get().getStartOffset() || offset >= firstRange.get().getEndOffset()) {
      return Optional.empty();
    }
    List<CachedRange> segments;
    if (firstRange.get().contains(offset, length)) {
      segments = java.util.Collections.singletonList(firstRange.get());
    } else {
      Optional<List<CachedRange>> contiguous =
          collectContiguousRanges(itemId, firstRange.get(), targetEnd);
      if (!contiguous.isPresent()) {
        return Optional.empty();
      }
      segments = contiguous.get();
    }
    for (CachedRange segment : segments) {
      segment.promote();
    }
    if (segments.size() == 1
        && firstRange.get().getStartOffset() == offset
        && firstRange.get().getLength() == length) {
      return Optional.of(
          firstRange
              .get()
              .getFuture()
              .thenApply(
                  buf -> {
                    if (!buf.isReadOnly() && buf.hasArray() && buf.arrayOffset() == 0) {
                      return buf.duplicate();
                    }
                    ByteBuffer target = allocate.apply(length);
                    if (target == null) {
                      return null;
                    }
                    target.put(buf.duplicate());
                    target.flip();
                    return target;
                  }));
    }
    List<CompletableFuture<ByteBuffer>> sliceFutures = new ArrayList<>(segments.size());
    long cursor = offset;
    for (CachedRange segment : segments) {
      if (cursor >= targetEnd) {
        break;
      }
      int sliceLength = (int) (Math.min(segment.getEndOffset(), targetEnd) - cursor);
      sliceFutures.add(segment.slice(cursor, sliceLength));
      cursor += sliceLength;
    }
    CompletableFuture<ByteBuffer> populatedFuture =
        CompletableFuture.allOf(sliceFutures.toArray(new CompletableFuture<?>[0]))
            .thenApply(
                unused -> {
                  ByteBuffer target = allocate.apply(length);
                  if (target == null) {
                    return null;
                  }
                  for (CompletableFuture<ByteBuffer> sliceFuture : sliceFutures) {
                    target.put(sliceFuture.join());
                  }
                  target.flip();
                  return target;
                });
    return Optional.of(populatedFuture);
  }

  /**
   * Copies bytes covering {@code position} into {@code dst} (waiting if the covering range is
   * currently in flight) and returns the number of bytes copied, or {@code 0} if no cached range
   * covers {@code position}.
   */
  public int copyInto(GcsItemId itemId, long position, ByteBuffer dst) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(dst, "dst cannot be null");
    int totalCopied = 0;
    while (dst.hasRemaining()) {
      long currentPos = position + totalCopied;
      Optional<CachedRange> covering = findRange(itemId, currentPos);
      if (!covering.isPresent() || !covering.get().contains(currentPos, 1)) {
        break;
      }
      CachedRange segment = covering.get();
      segment.promote();
      int copied = segment.copyInto(currentPos, dst);
      if (copied == 0) {
        break;
      }
      totalCopied += copied;
    }
    return totalCopied;
  }

  /** Evicts the cached range starting at {@code startOffset} for {@code itemId}, if present. */
  public void evictRange(GcsItemId itemId, long startOffset) {
    checkNotNull(itemId, "itemId cannot be null");
    removeRange(itemId, startOffset);
  }

  /** Returns whether a completed resident range covers {@code position}. */
  public boolean isCached(GcsItemId itemId, long position) {
    return isRangeCached(itemId, position, 1);
  }

  /** Returns whether the entire byte range {@code [offset, offset + length)} is resident. */
  public boolean isRangeCached(GcsItemId itemId, long offset, int length) {
    return getRangeCovering(itemId, offset, length).map(CachedRange::isDone).orElse(false);
  }

  /** Discards all cached ranges. */
  public void invalidateAll() {
    ranges.invalidateAll();
    offsetsByItem.clear();
  }

  private Optional<CachedRange> findRange(GcsItemId itemId, long offset) {
    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(itemId);
    if (itemOffsets == null) {
      return Optional.empty();
    }
    Long startOffset = itemOffsets.floor(offset);
    if (startOffset == null) {
      return Optional.empty();
    }
    CachedRange cached = ranges.getIfPresent(RangeKey.create(itemId, startOffset));
    if (cached == null) {
      itemOffsets.remove(startOffset);
      return Optional.empty();
    }
    return Optional.of(cached);
  }

  private void removeRange(GcsItemId itemId, long startOffset) {
    ranges.invalidate(RangeKey.create(itemId, startOffset));
    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(itemId);
    if (itemOffsets != null) {
      itemOffsets.remove(startOffset);
    }
  }

  /** Identifies a cached range by object and start offset. */
  @AutoValue
  abstract static class RangeKey {

    abstract GcsItemId getItemId();

    abstract long getStartOffset();

    static RangeKey create(GcsItemId itemId, long startOffset) {
      checkNotNull(itemId, "itemId cannot be null");
      return new AutoValue_PrefetchBufferCache_RangeKey(itemId, startOffset);
    }
  }
}
