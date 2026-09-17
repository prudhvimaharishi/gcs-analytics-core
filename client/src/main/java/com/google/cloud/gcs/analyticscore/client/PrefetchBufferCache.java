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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

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

  private static final int DEFAULT_BLOCK_SIZE_BYTES = 4 * 1024 * 1024;

  private final Cache<RangeKey, CachedRange> ranges;
  private final ConcurrentHashMap<GcsItemId, ConcurrentSkipListSet<Long>> offsetsByItem =
      new ConcurrentHashMap<>();
  private final int blockSizeBytes;

  /**
   * Creates a cache storing exact byte ranges.
   *
   * @param maxSizeBytes the maximum total size of retained buffers
   * @param ttlSeconds how long a range is retained after it was last read or written
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds) {
    this(maxSizeBytes, ttlSeconds, DEFAULT_BLOCK_SIZE_BYTES);
  }

  /**
   * Creates a cache.
   *
   * @param maxSizeBytes the maximum total size of retained buffers
   * @param ttlSeconds how long a range is retained after it was last read or written
   * @param blockSizeBytes nominal block size preserved for configuration compatibility
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds, int blockSizeBytes) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(ttlSeconds > 0, "ttlSeconds must be positive");
    checkArgument(blockSizeBytes > 0, "blockSizeBytes must be positive");
    this.blockSizeBytes = blockSizeBytes;
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

  /** Returns the configured nominal block size in bytes. */
  public int getBlockSizeBytes() {
    return blockSizeBytes;
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
    ConcurrentSkipListSet<Long> itemOffsets =
        offsetsByItem.computeIfAbsent(itemId, unused -> new ConcurrentSkipListSet<>());
    synchronized (itemOffsets) {
      if (getRangeCovering(itemId, startOffset, length).isPresent()) {
        return false;
      }
      CachedRange cachedRange = CachedRange.create(startOffset, startOffset + length, future);
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
      CachedRange cachedRange =
          CachedRange.create(
              rangeStart, rangeStart + length, CompletableFuture.completedFuture(readOnly));
      itemOffsets.add(rangeStart);
      ranges.put(RangeKey.create(itemId, rangeStart), cachedRange);
    }
  }

  /**
   * Returns the {@link CachedRange} (in-flight or resident) that completely covers {@code [offset,
   * offset + length)}, or {@code Optional.empty()} if none exists.
   */
  public Optional<CachedRange> getRangeCovering(GcsItemId itemId, long offset, int length) {
    checkNotNull(itemId, "itemId cannot be null");
    if (length < 0) {
      return Optional.empty();
    }
    return findRange(itemId, offset).filter(range -> range.contains(offset, length));
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
      Optional<CachedRange> covering = getRangeCovering(itemId, position + totalCopied, 1);
      if (!covering.isPresent()) {
        break;
      }
      int copied = covering.get().copyInto(position + totalCopied, dst);
      if (copied == 0) {
        break;
      }
      totalCopied += copied;
    }
    return totalCopied;
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
