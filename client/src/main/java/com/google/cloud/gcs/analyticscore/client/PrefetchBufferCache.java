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
                    forgetOffset(key.getItemId(), key.getStartOffset());
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
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(future, "future cannot be null");
    checkArgument(startOffset >= 0, "startOffset %s must be non-negative", startOffset);
    checkArgument(length > 0, "length %s must be positive", length);
    Optional<Boolean> registered = Optional.empty();
    while (!registered.isPresent()) {
      ConcurrentSkipListSet<Long> itemOffsets =
          offsetsByItem.computeIfAbsent(itemId, unused -> new ConcurrentSkipListSet<>());
      registered = tryRegister(itemId, itemOffsets, startOffset, length, future);
    }
    if (registered.get()) {
      CompletableFuture<ByteBuffer> unused =
          future.whenComplete(
              (buffer, error) -> {
                if (error != null || buffer == null) {
                  removeRange(itemId, startOffset);
                }
              });
    }
    return registered.get();
  }

  /**
   * Registers the range under the lock of {@code itemOffsets}.
   *
   * @return whether the range was registered, or empty if {@code itemOffsets} was dropped from
   *     {@link #offsetsByItem} after becoming empty and the caller must retry with a live set
   */
  private Optional<Boolean> tryRegister(
      GcsItemId itemId,
      ConcurrentSkipListSet<Long> itemOffsets,
      long startOffset,
      int length,
      CompletableFuture<ByteBuffer> future) {
    synchronized (itemOffsets) {
      if (offsetsByItem.get(itemId) != itemOffsets) {
        return Optional.empty();
      }
      if (getRangeCovering(itemId, startOffset, length).isPresent()) {
        return Optional.of(false);
      }
      itemOffsets.add(startOffset);
      ranges.put(
          RangeKey.create(itemId, startOffset),
          CachedRange.create(startOffset, startOffset + length, future));
      return Optional.of(true);
    }
  }

  /**
   * Returns the single {@link CachedRange} (in-flight or resident) that completely covers {@code
   * [offset, offset + length)}, or {@code Optional.empty()} if no single range covers the window.
   */
  public Optional<CachedRange> getRangeCovering(GcsItemId itemId, long offset, int length) {
    checkNotNull(itemId, "itemId cannot be null");
    if (length < 0) {
      return Optional.empty();
    }
    return findRange(itemId, offset).filter(range -> range.contains(offset, length));
  }

  /**
   * Returns a future holding the cached bytes for {@code [offset, offset + length)} when a single
   * cached range covers the window.
   *
   * <p>When the window matches the cached range exactly and the buffer is a writable heap buffer,
   * the buffer is returned without copying. Otherwise the target buffer is allocated once via
   * {@code allocate} and the bytes are copied into it.
   */
  public Optional<CompletableFuture<ByteBuffer>> consumeRange(
      GcsItemId itemId, long offset, int length, IntFunction<ByteBuffer> allocate) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(allocate, "allocate cannot be null");
    if (length <= 0) {
      return Optional.empty();
    }
    return getRangeCovering(itemId, offset, length)
        .map(
            range ->
                range.getFuture().thenApply(buf -> toTarget(range, buf, offset, length, allocate)));
  }

  private static ByteBuffer toTarget(
      CachedRange range,
      ByteBuffer buf,
      long offset,
      int length,
      IntFunction<ByteBuffer> allocate) {
    boolean exactMatch = range.getStartOffset() == offset && range.getLength() == length;
    if (exactMatch && !buf.isReadOnly() && buf.hasArray() && buf.arrayOffset() == 0) {
      return buf.duplicate();
    }
    ByteBuffer target = allocate.apply(length);
    if (target == null) {
      return null;
    }
    ByteBuffer view = buf.duplicate();
    view.position(view.position() + (int) (offset - range.getStartOffset()));
    view.limit(view.position() + length);
    target.put(view);
    target.flip();
    return target;
  }

  /**
   * Copies bytes covering {@code position} into {@code dst} (waiting if the covering range is
   * currently in flight) and returns the number of bytes copied, or {@code 0} if no cached range
   * covers {@code position}.
   *
   * <p>A cached range is evicted as soon as its last byte has been copied out.
   */
  public int copyInto(GcsItemId itemId, long position, ByteBuffer dst) {
    checkNotNull(itemId, "itemId cannot be null");
    checkNotNull(dst, "dst cannot be null");
    int totalCopied = 0;
    while (dst.hasRemaining()) {
      long currentPos = position + totalCopied;
      Optional<CachedRange> covering = getRangeCovering(itemId, currentPos, 1);
      if (!covering.isPresent()) {
        break;
      }
      CachedRange segment = covering.get();
      int copied = segment.copyInto(currentPos, dst);
      if (copied == 0) {
        break;
      }
      if (currentPos + copied >= segment.getEndOffset()) {
        removeRange(itemId, segment.getStartOffset());
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

  /** Evicts every cached range of {@code itemId} that overlaps {@code [startOffset, endOffset)}. */
  public void evictRangeWindow(GcsItemId itemId, long startOffset, long endOffset) {
    checkNotNull(itemId, "itemId cannot be null");
    if (endOffset <= startOffset) {
      return;
    }
    findRange(itemId, startOffset)
        .filter(range -> range.getEndOffset() > startOffset)
        .ifPresent(range -> removeRange(itemId, range.getStartOffset()));
    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(itemId);
    if (itemOffsets == null) {
      return;
    }
    for (Long rangeStart : new ArrayList<>(itemOffsets.subSet(startOffset, endOffset))) {
      removeRange(itemId, rangeStart);
    }
  }

  /** Evicts every cached range of {@code itemId}. */
  public void evictAllForItem(GcsItemId itemId) {
    checkNotNull(itemId, "itemId cannot be null");
    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(itemId);
    if (itemOffsets == null) {
      return;
    }
    for (Long rangeStart : new ArrayList<>(itemOffsets)) {
      removeRange(itemId, rangeStart);
    }
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
      forgetOffset(itemId, startOffset);
      return Optional.empty();
    }
    return Optional.of(cached);
  }

  private void removeRange(GcsItemId itemId, long startOffset) {
    ranges.invalidate(RangeKey.create(itemId, startOffset));
    forgetOffset(itemId, startOffset);
  }

  /**
   * Drops {@code startOffset} from the offset index of {@code itemId}, removing the item's entry
   * altogether once it holds no offsets so that closed objects do not leak map entries.
   */
  private void forgetOffset(GcsItemId itemId, long startOffset) {
    ConcurrentSkipListSet<Long> itemOffsets = offsetsByItem.get(itemId);
    if (itemOffsets == null) {
      return;
    }
    synchronized (itemOffsets) {
      itemOffsets.remove(startOffset);
      if (itemOffsets.isEmpty()) {
        offsetsByItem.remove(itemId, itemOffsets);
      }
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
