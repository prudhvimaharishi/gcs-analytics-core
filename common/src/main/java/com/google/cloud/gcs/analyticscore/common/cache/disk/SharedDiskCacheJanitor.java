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

package com.google.cloud.gcs.analyticscore.common.cache.disk;

import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background maintenance for a {@link SharedDiskCache}. Every process sharing the cache directory
 * schedules the janitor, but a host-wide advisory lock elects a single runner per sweep, so the
 * work is done once per host regardless of how many executor processes are running.
 *
 * <p>A sweep walks the entry tree once and, in that single pass: deletes expired entries (when TTL
 * is enabled), evicts least-recently-used entries down to the low watermark when usage exceeds the
 * high watermark, publishes the reconciled usage total to the stats file for other processes'
 * admission control, and removes orphaned temporary files left behind by crashed writers.
 */
final class SharedDiskCacheJanitor {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDiskCacheJanitor.class);

  /** Temporary files older than this are considered abandoned by a crashed writer. */
  private static final long STALE_TEMP_FILE_AGE_MILLIS = TimeUnit.MINUTES.toMillis(15);

  private final SharedDiskCache cache;
  private final SharedDiskCacheOptions options;

  SharedDiskCacheJanitor(SharedDiskCache cache, SharedDiskCacheOptions options) {
    this.cache = cache;
    this.options = options;
  }

  /**
   * Runs a sweep if this process wins the host-wide janitor lock; returns immediately otherwise.
   * The lock is released automatically by the OS if the process dies mid-sweep.
   */
  void runIfElected() {
    try (FileChannel lockChannel =
        FileChannel.open(
            cache.getJanitorLockFile(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      FileLock lock;
      try {
        lock = lockChannel.tryLock();
      } catch (OverlappingFileLockException e) {
        return;
      }
      if (lock == null) {
        return;
      }
      sweep();
    } catch (IOException e) {
      LOG.warn("Disk cache janitor sweep failed", e);
    }
  }

  private void sweep() throws IOException {
    long now = System.currentTimeMillis();
    List<CacheEntry> entries = collectLiveEntries(now);
    long totalBytes = entries.stream().mapToLong(entry -> entry.sizeBytes).sum();
    totalBytes -= evictIfOverHighWatermark(entries, totalBytes);
    writeStatsFile(totalBytes, now);
    cache.onUsageReconciled(totalBytes, now);
    deleteStaleTempFiles(now);
  }

  /** Walks the entry tree, deleting expired entries and returning the live ones. */
  private List<CacheEntry> collectLiveEntries(long now) throws IOException {
    List<CacheEntry> entries = new ArrayList<>();
    try (Stream<Path> paths = Files.walk(cache.getEntriesDirectory())) {
      paths
          .filter(path -> path.toString().endsWith(SharedDiskCache.ENTRY_FILE_SUFFIX))
          .forEach(
              path -> {
                try {
                  BasicFileAttributes attributes =
                      Files.readAttributes(path, BasicFileAttributes.class);
                  if (isExpired(path, now)) {
                    SharedDiskCache.deleteQuietly(path);
                    return;
                  }
                  entries.add(
                      new CacheEntry(
                          path, attributes.size(), attributes.lastModifiedTime().toMillis()));
                } catch (IOException e) {
                  // The entry was deleted concurrently by another process; skip it.
                }
              });
    }
    return entries;
  }

  /**
   * Evicts least-recently-used entries down to the low watermark when {@code totalBytes} exceeds
   * the high watermark. Returns the number of bytes evicted.
   */
  private long evictIfOverHighWatermark(List<CacheEntry> entries, long totalBytes) {
    long highWatermarkBytes =
        (long) (options.getMaxSizeBytes() * options.getEvictionHighWatermark());
    if (totalBytes <= highWatermarkBytes) {
      return 0;
    }
    long targetBytes = (long) (options.getMaxSizeBytes() * options.getEvictionLowWatermark());
    entries.sort(Comparator.comparingLong(entry -> entry.lastAccessMillis));
    long evictedBytes = 0;
    for (CacheEntry entry : entries) {
      if (totalBytes - evictedBytes <= targetBytes) {
        break;
      }
      SharedDiskCache.deleteQuietly(entry.path);
      evictedBytes += entry.sizeBytes;
    }
    cache.recordMetric(Metric.DISK_CACHE_EVICTED_BYTES, evictedBytes);
    return evictedBytes;
  }

  /**
   * Returns whether the entry's header creation time exceeds the TTL. Unreadable headers are
   * treated as expired so corrupt files do not survive sweeps.
   */
  private boolean isExpired(Path entryPath, long now) {
    if (options.getTtlMillis() <= 0) {
      return false;
    }
    try (FileChannel channel = FileChannel.open(entryPath, StandardOpenOption.READ)) {
      ByteBuffer headerPrefix = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
      while (headerPrefix.hasRemaining()) {
        if (channel.read(headerPrefix) == -1) {
          return true;
        }
      }
      headerPrefix.flip();
      if (headerPrefix.getInt() != SharedDiskCache.ENTRY_MAGIC) {
        return true;
      }
      return now - headerPrefix.getLong() > options.getTtlMillis();
    } catch (IOException e) {
      return true;
    }
  }

  /** Atomically publishes the reconciled usage total for other processes' admission control. */
  private void writeStatsFile(long totalBytes, long writtenAtMillis) throws IOException {
    ByteBuffer stats = ByteBuffer.allocate(2 * Long.BYTES);
    stats.putLong(totalBytes);
    stats.putLong(writtenAtMillis);
    stats.flip();
    Path tempPath =
        cache.getTempDirectory().resolve("usage.stats" + SharedDiskCache.TEMP_FILE_SUFFIX);
    try (FileChannel channel =
        FileChannel.open(
            tempPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      while (stats.hasRemaining()) {
        channel.write(stats);
      }
      channel.force(true);
    }
    Files.move(
        tempPath,
        cache.getStatsFile(),
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING);
  }

  /** Removes temporary files abandoned by writers that crashed before their atomic rename. */
  private void deleteStaleTempFiles(long now) throws IOException {
    try (Stream<Path> paths = Files.list(cache.getTempDirectory())) {
      paths.forEach(
          path -> {
            try {
              long ageMillis = now - Files.getLastModifiedTime(path).toMillis();
              if (ageMillis > STALE_TEMP_FILE_AGE_MILLIS) {
                SharedDiskCache.deleteQuietly(path);
              }
            } catch (NoSuchFileException e) {
              // Renamed or deleted concurrently; nothing to do.
            } catch (IOException e) {
              LOG.debug("Failed to inspect temp file {}", path, e);
            }
          });
    }
  }

  /** A live cache entry observed during a sweep. */
  private static final class CacheEntry {
    final Path path;
    final long sizeBytes;
    final long lastAccessMillis;

    CacheEntry(Path path, long sizeBytes, long lastAccessMillis) {
      this.path = path;
      this.sizeBytes = sizeBytes;
      this.lastAccessMillis = lastAccessMillis;
    }
  }
}
