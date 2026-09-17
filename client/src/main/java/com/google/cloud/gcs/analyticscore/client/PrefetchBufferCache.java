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
import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Holds speculatively fetched bytes as fixed-size blocks addressed by absolute file position.
 *
 * <p>The object is partitioned into a grid of {@code blockSizeBytes} blocks, so the block holding a
 * position is pure arithmetic rather than a property of the file format. Callers therefore never
 * need to know where a block begins, and bytes fetched for one purpose are reusable by any later
 * read that lands in the same block.
 *
 * <p>Entries are bounded by total bytes and expire once they have been idle for the configured
 * time, because a block nobody comes back to is unlikely to be wanted at all. Retention is counted
 * from the last read rather than from the download, so a block stays resident while the engine is
 * still working through it however long that takes.
 *
 * <p>This class is thread-safe.
 */
public final class PrefetchBufferCache {

  private static final ConcurrentMap<CacheConfig, PrefetchBufferCache> SHARED_INSTANCES =
      new ConcurrentHashMap<>();

  private final Cache<BlockKey, ByteBuffer> blocks;
  private final Set<BlockKey> claimedBlocks = ConcurrentHashMap.newKeySet();
  private final ConcurrentMap<BlockKey, CompletableFuture<ByteBuffer>> inFlightBlocks =
      new ConcurrentHashMap<>();
  private final int blockSizeBytes;

  /**
   * Creates a cache.
   *
   * @param maxSizeBytes the maximum total size of retained blocks
   * @param ttlSeconds how long a block is retained after it was last read or written
   * @param blockSizeBytes the fixed size of a block
   */
  public PrefetchBufferCache(long maxSizeBytes, long ttlSeconds, int blockSizeBytes) {
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(ttlSeconds > 0, "ttlSeconds must be positive");
    checkArgument(blockSizeBytes > 0, "blockSizeBytes must be positive");
    this.blockSizeBytes = blockSizeBytes;
    this.blocks =
        Caffeine.newBuilder()
            .maximumWeight(maxSizeBytes)
            .weigher((BlockKey key, ByteBuffer value) -> value.remaining())
            .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
            .build();
  }

  /**
   * Returns the cache shared by the whole process for the given configuration.
   *
   * <p>Query engines hand every task its own file system instance, and a cache owned by that
   * instance means {@code maxSizeBytes} is a budget per task rather than per process: a host
   * running a dozen tasks would hold a dozen full-size caches. Sharing one cache instead keeps the
   * configured bound meaningful and lets tasks reading the same object reuse each other's blocks.
   *
   * @param maxSizeBytes the maximum total size of retained blocks
   * @param ttlSeconds how long a block is retained after it was last read or written
   * @param blockSizeBytes the fixed size of a block
   */
  public static PrefetchBufferCache getSharedInstance(
      long maxSizeBytes, long ttlSeconds, int blockSizeBytes) {
    CacheConfig config = CacheConfig.create(maxSizeBytes, ttlSeconds, blockSizeBytes);
    return SHARED_INSTANCES.computeIfAbsent(
        config,
        key ->
            new PrefetchBufferCache(
                key.getMaxSizeBytes(), key.getTtlSeconds(), key.getBlockSizeBytes()));
  }

  /** Returns the fixed size of a block in bytes. */
  public int getBlockSizeBytes() {
    return blockSizeBytes;
  }

  /** Returns the position of the first byte of the block containing {@code position}. */
  public long alignDown(long position) {
    return toBlockIndex(position) * (long) blockSizeBytes;
  }

  /**
   * Copies bytes from the block containing {@code position} into {@code dst} and returns the number
   * of bytes copied, or {@code 0} if that block is not resident.
   *
   * <p>A single call never crosses a block boundary. Callers reading more than one block invoke
   * this in a loop, advancing {@code position} by the returned count, so a read is served for as
   * long as consecutive blocks remain resident.
   */
  public int copyInto(GcsItemId itemId, long position, ByteBuffer dst) {
    ByteBuffer cached = blocks.getIfPresent(BlockKey.create(itemId, toBlockIndex(position)));
    if (cached == null) {
      return 0;
    }
    int offsetInBlock = (int) (position - alignDown(position));
    if (offsetInBlock >= cached.remaining()) {
      return 0;
    }
    ByteBuffer view = cached.duplicate();
    view.position(view.position() + offsetInBlock);
    int copiedBytes = Math.min(dst.remaining(), view.remaining());
    view.limit(view.position() + copiedBytes);
    dst.put(view);
    return copiedBytes;
  }

  /**
   * Stores a block-aligned range, splitting it into one entry per block so that a later read
   * resolves any part of it.
   *
   * <p>The data is retained as read-only views rather than copies, so the caller must not mutate
   * {@code data} afterwards. Avoiding the copy matters because ranges can be tens of megabytes and
   * the fetching code allocates a fresh buffer per range anyway.
   *
   * @throws IllegalArgumentException if {@code rangeStart} is not on a block boundary
   */
  public void putRange(GcsItemId itemId, long rangeStart, ByteBuffer data) {
    checkNotNull(data, "data cannot be null");
    checkArgument(
        rangeStart == alignDown(rangeStart),
        "rangeStart %s must be aligned to blockSizeBytes %s",
        rangeStart,
        blockSizeBytes);
    ByteBuffer source = data.asReadOnlyBuffer();
    long blockStart = rangeStart;
    while (source.hasRemaining()) {
      int length = Math.min(blockSizeBytes, source.remaining());
      ByteBuffer block = source.slice();
      block.limit(length);
      blocks.put(BlockKey.create(itemId, toBlockIndex(blockStart)), block);
      source.position(source.position() + length);
      blockStart += length;
    }
  }

  /** Returns whether the block containing {@code position} is resident. */
  public boolean isCached(GcsItemId itemId, long position) {
    return blocks.getIfPresent(BlockKey.create(itemId, toBlockIndex(position))) != null;
  }

  /**
   * Attempts to take ownership of fetching the block containing {@code position}.
   *
   * <p>Returns {@code true} for exactly one caller per block until {@link #releaseClaim} is
   * invoked. A residency check cannot serve this purpose on its own, because a block stays absent
   * for the whole window between issuing its request and the bytes arriving; without a claim every
   * read in that window would issue a duplicate fetch.
   *
   * <p>Claims are held here rather than per stream so that two streams reading the same object do
   * not both fetch the same block.
   */
  public boolean tryClaim(GcsItemId itemId, long position) {
    return claimedBlocks.add(BlockKey.create(itemId, toBlockIndex(position)));
  }

  /** Releases a claim taken by {@link #tryClaim}, along with any fetch registered for it. */
  public void releaseClaim(GcsItemId itemId, long position) {
    BlockKey key = BlockKey.create(itemId, toBlockIndex(position));
    claimedBlocks.remove(key);
    inFlightBlocks.remove(key);
  }

  /** Returns whether a fetch for the block containing {@code position} is currently claimed. */
  public boolean isClaimed(GcsItemId itemId, long position) {
    return claimedBlocks.contains(BlockKey.create(itemId, toBlockIndex(position)));
  }

  /**
   * Records the stage that settles once the claimed block containing {@code position} has been
   * stored, so that other readers can wait for it instead of fetching the same bytes again.
   *
   * <p>The registry lives here rather than with the component that issued the fetch because claims
   * do too: a reader that skips a block because someone else claimed it has to be able to find that
   * someone else's request, even when the two belong to different streams.
   */
  public void registerInFlight(
      GcsItemId itemId, long position, CompletableFuture<ByteBuffer> published) {
    checkNotNull(published, "published cannot be null");
    inFlightBlocks.put(BlockKey.create(itemId, toBlockIndex(position)), published);
  }

  /**
   * Returns the stage that settles once the block containing {@code position} has been stored, or
   * {@code null} when no fetch for it is outstanding.
   */
  @Nullable
  public CompletableFuture<ByteBuffer> getInFlight(GcsItemId itemId, long position) {
    return inFlightBlocks.get(BlockKey.create(itemId, toBlockIndex(position)));
  }

  /** Discards all cached blocks, claims and outstanding fetches. */
  public void invalidateAll() {
    blocks.invalidateAll();
    claimedBlocks.clear();
    inFlightBlocks.clear();
  }

  private long toBlockIndex(long position) {
    return position / blockSizeBytes;
  }

  /** Identifies a cached block by object and block index. */
  @AutoValue
  abstract static class BlockKey {

    abstract GcsItemId getItemId();

    abstract long getBlockIndex();

    static BlockKey create(GcsItemId itemId, long blockIndex) {
      checkNotNull(itemId, "itemId cannot be null");
      return new AutoValue_PrefetchBufferCache_BlockKey(itemId, blockIndex);
    }
  }

  /** Identifies a shared cache by the configuration it was created with. */
  @AutoValue
  abstract static class CacheConfig {

    abstract long getMaxSizeBytes();

    abstract long getTtlSeconds();

    abstract int getBlockSizeBytes();

    static CacheConfig create(long maxSizeBytes, long ttlSeconds, int blockSizeBytes) {
      return new AutoValue_PrefetchBufferCache_CacheConfig(
          maxSizeBytes, ttlSeconds, blockSizeBytes);
    }
  }
}
