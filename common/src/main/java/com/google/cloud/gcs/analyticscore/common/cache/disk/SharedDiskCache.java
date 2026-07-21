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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.cache.ThrowingSupplier;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A worker-level (host-shared) cache that stores entries as files under a single directory shared
 * by every process on the host. The filesystem is the only coordination mechanism:
 *
 * <ul>
 *   <li><b>Publishing:</b> entries are written to a private temporary file and atomically renamed
 *       into place, so a visible entry file is always complete. Concurrent writers of the same key
 *       are harmless because entries are keyed by immutable content (e.g. a GCS object generation).
 *   <li><b>Reading:</b> the happy path takes no locks. POSIX unlink semantics let the janitor
 *       delete an entry while another process is reading it; the reader's open file descriptor
 *       remains valid.
 *   <li><b>Download deduplication:</b> {@link #getOrLoad} takes a per-key advisory file lock before
 *       loading. Processes that lose the lock race read through to the source instead of waiting,
 *       so the cache can only ever help, never block.
 *   <li><b>Capacity:</b> a strict size limit is enforced by admission control — writes are skipped
 *       when the usage estimate reaches the maximum size — while a background janitor (one elected
 *       process per host) restores headroom by evicting least-recently-used entries.
 *   <li><b>TTL:</b> entry creation time is stored in the entry header and checked lazily on read;
 *       the janitor also sweeps expired entries in the background.
 * </ul>
 *
 * <p>Entry files carry a fixed-size header (magic, creation time, payload length, CRC32C) so
 * truncated or corrupt entries are detected and deleted on read. The file's last-modified time is
 * used as the recency signal for LRU eviction and is refreshed (throttled) on cache hits.
 *
 * <p>Instances are shared JVM-wide per cache directory via {@link #getOrCreate}. The background
 * maintenance thread is a daemon and lives for the remainder of the JVM. All failures on the cache
 * path are contained: a broken disk cache degrades to a read-through miss, never an error.
 */
public final class SharedDiskCache {

  private static final Logger LOG = LoggerFactory.getLogger(SharedDiskCache.class);

  private static final ConcurrentHashMap<Path, SharedDiskCache> instances =
      new ConcurrentHashMap<>();

  /** Identifies entry files written by this cache ("GCSC" in ASCII). */
  static final int ENTRY_MAGIC = 0x47435343;

  /** Header layout: magic (int), creation epoch millis (long), payload length (int), CRC (int). */
  static final int ENTRY_HEADER_SIZE_BYTES =
      Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

  static final String ENTRY_FILE_SUFFIX = ".bin";
  static final String TEMP_FILE_SUFFIX = ".tmp";

  /** Minimum age of an entry's last-modified time before a cache hit refreshes it. */
  private static final long TOUCH_THROTTLE_MILLIS = TimeUnit.MINUTES.toMillis(2);

  private final SharedDiskCacheOptions options;
  private final Path entriesDirectory;
  private final Path locksDirectory;
  private final Path tempDirectory;
  private final Path statsFile;
  private final Path janitorLockFile;
  private final SharedDiskCacheJanitor janitor;
  private final ScheduledExecutorService maintenanceExecutor;
  private final AtomicBoolean evictionRequested = new AtomicBoolean(false);

  /** Host-wide usage (in bytes) as of the last janitor reconciliation. */
  private volatile long reconciledUsageBytes;

  /** Timestamp of the stats file backing {@link #reconciledUsageBytes}. */
  private volatile long reconciledAtMillis;

  /** Bytes written by this process since the last reconciliation. */
  private final AtomicLong pendingWriteBytes = new AtomicLong();

  /**
   * Telemetry of the most recent {@link #getOrCreate} caller. Refreshed on every call so that
   * metrics keep flowing after the filesystem instance that first created this cache is closed.
   */
  private volatile Telemetry telemetry;

  private SharedDiskCache(Path rootDirectory, SharedDiskCacheOptions options) throws IOException {
    this.options = options;
    this.entriesDirectory = rootDirectory.resolve("entries");
    this.locksDirectory = rootDirectory.resolve("locks");
    this.tempDirectory = rootDirectory.resolve("tmp");
    this.statsFile = rootDirectory.resolve("usage.stats");
    this.janitorLockFile = rootDirectory.resolve("janitor.lock");
    createCacheDirectories(rootDirectory);
    this.janitor = new SharedDiskCacheJanitor(this, options);
    this.maintenanceExecutor =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setNameFormat("gcs-shared-disk-cache-maintenance-%d")
                .setDaemon(true)
                .build());
    scheduleMaintenance();
  }

  /**
   * Returns the JVM-wide {@link SharedDiskCache} for the directory in {@code options}, creating it
   * on first use. Later callers share the first instance; their {@code options} are ignored apart
   * from the directory, and their {@code telemetry} becomes the metrics destination.
   *
   * @throws IOException if the cache directories cannot be created.
   */
  public static SharedDiskCache getOrCreate(SharedDiskCacheOptions options, Telemetry telemetry)
      throws IOException {
    checkNotNull(options, "options cannot be null");
    checkNotNull(telemetry, "telemetry cannot be null");
    Path rootDirectory = Paths.get(options.getCacheDirectory()).toAbsolutePath().normalize();
    try {
      SharedDiskCache cache =
          instances.computeIfAbsent(
              rootDirectory,
              directory -> {
                try {
                  return new SharedDiskCache(directory, options);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
      cache.telemetry = telemetry;
      return cache;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  /**
   * Returns the value for {@code key}, loading it from {@code loader} on a cache miss.
   *
   * <p>On a miss this method takes a per-key advisory file lock so that only one process on the
   * host downloads the value; a process that loses the lock race invokes {@code loader} for its own
   * result without caching it. The returned buffer is positioned at zero and owned by the caller.
   *
   * @throws E if the {@code loader} throws; the failure is never cached.
   */
  public <E extends Exception> ByteBuffer getOrLoad(
      String key, ThrowingSupplier<ByteBuffer, E> loader) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(loader, "loader cannot be null");
    Optional<ByteBuffer> cached = read(key);
    if (cached.isPresent()) {
      recordMetric(Metric.DISK_CACHE_HIT, 1);
      return cached.get();
    }
    recordMetric(Metric.DISK_CACHE_MISS, 1);

    Optional<KeyLock> keyLock = tryLockKey(key);
    if (!keyLock.isPresent()) {
      // Another process is loading this key right now; read through without caching.
      return loader.get();
    }
    try {
      // Re-check under the lock: the previous holder may have published the entry already.
      Optional<ByteBuffer> published = read(key);
      if (published.isPresent()) {
        recordMetric(Metric.DISK_CACHE_HIT, 1);
        return published.get();
      }
      ByteBuffer loaded = loader.get();
      write(key, loaded.duplicate());
      return loaded;
    } finally {
      keyLock.get().close();
    }
  }

  /** Deletes the entry for {@code key}, if present. */
  public void invalidate(String key) {
    checkNotNull(key, "key cannot be null");
    deleteQuietly(entryPath(key));
  }

  /**
   * Returns the cached value for {@code key}, or empty on a miss. Expired or corrupt entries are
   * deleted and reported as a miss.
   */
  Optional<ByteBuffer> read(String key) {
    Path entryPath = entryPath(key);
    try (FileChannel channel = FileChannel.open(entryPath, StandardOpenOption.READ)) {
      Optional<ByteBuffer> payload = readValidEntry(channel);
      if (!payload.isPresent()) {
        deleteQuietly(entryPath);
        return Optional.empty();
      }
      touchIfStale(entryPath);
      return payload;
    } catch (NoSuchFileException e) {
      return Optional.empty();
    } catch (IOException e) {
      LOG.warn("Failed to read disk cache entry {}", entryPath, e);
      return Optional.empty();
    }
  }

  /**
   * Writes an entry for {@code key}, subject to admission control: the write is skipped when it
   * would push the usage estimate past the maximum cache size. Failures are logged and swallowed —
   * a failed cache write must never fail the read it was piggybacking on.
   */
  void write(String key, ByteBuffer value) {
    long entrySizeBytes = ENTRY_HEADER_SIZE_BYTES + value.remaining();
    if (currentUsageBytes() + entrySizeBytes > options.getMaxSizeBytes()) {
      recordMetric(Metric.DISK_CACHE_WRITE_SKIPPED, 1);
      requestEviction();
      return;
    }
    Path entryPath = entryPath(key);
    Path tempPath = tempDirectory.resolve(UUID.randomUUID() + TEMP_FILE_SUFFIX);
    try {
      Files.createDirectories(entryPath.getParent());
      try (FileChannel channel =
          FileChannel.open(tempPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
        writeFully(channel, buildEntryHeader(value));
        writeFully(channel, value);
        channel.force(true);
      }
      Files.move(
          tempPath, entryPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      pendingWriteBytes.addAndGet(entrySizeBytes);
      if (currentUsageBytes() > options.getMaxSizeBytes() * options.getEvictionHighWatermark()) {
        requestEviction();
      }
    } catch (IOException e) {
      LOG.warn("Failed to write disk cache entry {}", entryPath, e);
      deleteQuietly(tempPath);
    }
  }

  /** Applies the host-wide usage reported by the janitor and resets this process's local delta. */
  void onUsageReconciled(long usageBytes, long reconciledAtMillis) {
    this.reconciledUsageBytes = usageBytes;
    this.reconciledAtMillis = reconciledAtMillis;
    this.pendingWriteBytes.set(0);
  }

  void recordMetric(Metric metric, long value) {
    Telemetry currentTelemetry = telemetry;
    if (currentTelemetry != null) {
      currentTelemetry.recordMetric(metric, value, Collections.emptyMap());
    }
  }

  Path getEntriesDirectory() {
    return entriesDirectory;
  }

  Path getTempDirectory() {
    return tempDirectory;
  }

  Path getStatsFile() {
    return statsFile;
  }

  Path getJanitorLockFile() {
    return janitorLockFile;
  }

  /**
   * Estimates the current host-wide usage: the last janitor reconciliation plus bytes this process
   * has written since. Writes by other processes between reconciliations are not visible, so the
   * estimate can lag by up to one maintenance period.
   */
  private long currentUsageBytes() {
    return reconciledUsageBytes + pendingWriteBytes.get();
  }

  private void scheduleMaintenance() {
    // A random initial delay desynchronizes the executor processes sharing this cache directory.
    long initialDelayMillis =
        ThreadLocalRandom.current().nextLong(options.getMaintenancePeriodMillis() / 2 + 1);
    maintenanceExecutor.scheduleWithFixedDelay(
        this::runMaintenance,
        initialDelayMillis,
        options.getMaintenancePeriodMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void runMaintenance() {
    evictionRequested.set(false);
    try {
      refreshUsageFromStatsFile();
      janitor.runIfElected();
    } catch (RuntimeException e) {
      LOG.warn("Disk cache maintenance run failed", e);
    }
  }

  /** Schedules an out-of-band maintenance run, coalescing concurrent requests. */
  private void requestEviction() {
    if (evictionRequested.compareAndSet(false, true)) {
      maintenanceExecutor.execute(this::runMaintenance);
    }
  }

  /** Adopts the usage total from the stats file if a janitor has written a newer one. */
  private void refreshUsageFromStatsFile() {
    try {
      ByteBuffer stats = ByteBuffer.wrap(Files.readAllBytes(statsFile));
      if (stats.remaining() != 2 * Long.BYTES) {
        return;
      }
      long usageBytes = stats.getLong();
      long writtenAtMillis = stats.getLong();
      if (writtenAtMillis > reconciledAtMillis) {
        onUsageReconciled(usageBytes, writtenAtMillis);
      }
    } catch (NoSuchFileException e) {
      // No janitor has completed a scan yet.
    } catch (IOException e) {
      LOG.warn("Failed to read disk cache stats file {}", statsFile, e);
    }
  }

  private Optional<ByteBuffer> readValidEntry(FileChannel channel) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(ENTRY_HEADER_SIZE_BYTES);
    if (!readFully(channel, header)) {
      return Optional.empty();
    }
    header.flip();
    if (header.getInt() != ENTRY_MAGIC) {
      return Optional.empty();
    }
    long createdAtMillis = header.getLong();
    int payloadLength = header.getInt();
    int expectedCrc = header.getInt();
    if (isExpired(createdAtMillis)) {
      return Optional.empty();
    }
    if (payloadLength < 0 || payloadLength != channel.size() - ENTRY_HEADER_SIZE_BYTES) {
      return Optional.empty();
    }
    ByteBuffer payload = ByteBuffer.allocate(payloadLength);
    if (!readFully(channel, payload)) {
      return Optional.empty();
    }
    payload.flip();
    if (computeCrc(payload) != expectedCrc) {
      return Optional.empty();
    }
    return Optional.of(payload);
  }

  private boolean isExpired(long createdAtMillis) {
    return options.getTtlMillis() > 0
        && System.currentTimeMillis() - createdAtMillis > options.getTtlMillis();
  }

  /**
   * Refreshes the entry's last-modified time — the LRU recency signal — at most once per {@link
   * #TOUCH_THROTTLE_MILLIS} so hot entries do not generate constant metadata writes.
   */
  private void touchIfStale(Path entryPath) {
    try {
      long now = System.currentTimeMillis();
      if (now - Files.getLastModifiedTime(entryPath).toMillis() > TOUCH_THROTTLE_MILLIS) {
        Files.setLastModifiedTime(entryPath, FileTime.fromMillis(now));
      }
    } catch (IOException e) {
      // The entry may have been evicted concurrently; recency refresh is best-effort.
    }
  }

  /**
   * Tries to take the host-wide advisory lock for {@code key}. Returns empty if any process
   * (including this one) already holds it. The OS releases the lock automatically if the holding
   * process dies, so stale locks cannot occur.
   */
  private Optional<KeyLock> tryLockKey(String key) {
    Path lockPath = locksDirectory.resolve(hashKey(key) + ".lock");
    FileChannel channel = null;
    try {
      channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
      FileLock lock = channel.tryLock();
      if (lock == null) {
        channel.close();
        return Optional.empty();
      }
      return Optional.of(new KeyLock(channel, lock));
    } catch (OverlappingFileLockException e) {
      closeQuietly(channel);
      return Optional.empty();
    } catch (IOException e) {
      LOG.warn("Failed to acquire disk cache key lock {}", lockPath, e);
      closeQuietly(channel);
      return Optional.empty();
    }
  }

  private Path entryPath(String key) {
    String hash = hashKey(key);
    return entriesDirectory
        .resolve(hash.substring(0, 2))
        .resolve(hash.substring(2, 4))
        .resolve(hash + ENTRY_FILE_SUFFIX);
  }

  private static String hashKey(String key) {
    return Hashing.sha256().hashString(key, StandardCharsets.UTF_8).toString();
  }

  private static ByteBuffer buildEntryHeader(ByteBuffer payload) {
    ByteBuffer header = ByteBuffer.allocate(ENTRY_HEADER_SIZE_BYTES);
    header.putInt(ENTRY_MAGIC);
    header.putLong(System.currentTimeMillis());
    header.putInt(payload.remaining());
    header.putInt(computeCrc(payload));
    header.flip();
    return header;
  }

  private static int computeCrc(ByteBuffer payload) {
    CRC32C crc = new CRC32C();
    crc.update(payload.duplicate());
    return (int) crc.getValue();
  }

  private static boolean readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      if (channel.read(buffer) == -1) {
        return false;
      }
    }
    return true;
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  private void createCacheDirectories(Path rootDirectory) throws IOException {
    Files.createDirectories(rootDirectory);
    restrictToOwner(rootDirectory);
    Files.createDirectories(entriesDirectory);
    Files.createDirectories(locksDirectory);
    Files.createDirectories(tempDirectory);
  }

  /** Keeps cached data private to the current user; a no-op on non-POSIX filesystems. */
  private static void restrictToOwner(Path directory) {
    try {
      Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwx------"));
    } catch (UnsupportedOperationException | IOException e) {
      LOG.debug("Could not restrict permissions on {}", directory, e);
    }
  }

  static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOG.debug("Failed to delete {}", path, e);
    }
  }

  private static void closeQuietly(FileChannel channel) {
    if (channel != null) {
      try {
        channel.close();
      } catch (IOException e) {
        // Ignore.
      }
    }
  }

  /** A held per-key advisory lock; closing releases the lock and its underlying channel. */
  private static final class KeyLock implements AutoCloseable {
    private final FileChannel channel;
    private final FileLock lock;

    KeyLock(FileChannel channel, FileLock lock) {
      this.channel = channel;
      this.lock = lock;
    }

    @Override
    public void close() {
      try {
        lock.release();
      } catch (IOException e) {
        // The lock dies with the channel below regardless.
      }
      closeQuietly(channel);
    }
  }
}
