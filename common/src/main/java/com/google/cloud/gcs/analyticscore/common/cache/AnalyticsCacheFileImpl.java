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

package com.google.cloud.gcs.analyticscore.common.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnalyticsCacheFileImpl<K> implements AnalyticsCache<K, ByteBuffer> {

  private static final double DEFAULT_HIGH_WATERMARK_RATIO = 0.90;
  private static final double DEFAULT_LOW_WATERMARK_RATIO = 0.75;
  private static final long DEFAULT_TOUCH_THROTTLE_MILLIS = 60_000L;
  private static final long STALE_TEMP_FILE_THRESHOLD_MILLIS = TimeUnit.MINUTES.toMillis(10);
  private static final String CACHE_FILE_EXTENSION = ".cache";
  private static final String TEMP_FILE_PREFIX = "data_";
  private static final String TEMP_FILE_MARKER = ".tmp.";

  private final Path cacheDirectory;
  private final long maxSizeBytes;
  private final long highWatermarkBytes;
  private final long lowWatermarkBytes;
  private final long touchThrottleMillis;
  private final Function<K, String> keyToStringFunction;
  private final Path evictionLockPath;

  public static <K> AnalyticsCacheFileImpl<K> create(
      Path cacheDirectory, long maxSizeBytes, Function<K, String> keyToStringFunction) {
    return new AnalyticsCacheFileImpl<>(
        cacheDirectory,
        maxSizeBytes,
        DEFAULT_HIGH_WATERMARK_RATIO,
        DEFAULT_LOW_WATERMARK_RATIO,
        DEFAULT_TOUCH_THROTTLE_MILLIS,
        keyToStringFunction);
  }

  public static <K> AnalyticsCacheFileImpl<K> create(
      Path cacheDirectory,
      long maxSizeBytes,
      double highWatermarkRatio,
      double lowWatermarkRatio,
      long touchThrottleMillis,
      Function<K, String> keyToStringFunction) {
    return new AnalyticsCacheFileImpl<>(
        cacheDirectory,
        maxSizeBytes,
        highWatermarkRatio,
        lowWatermarkRatio,
        touchThrottleMillis,
        keyToStringFunction);
  }

  @Override
  public Optional<ByteBuffer> get(K key) {
    checkNotNull(key, "key cannot be null");
    Path path = getCacheFilePath(key);
    if (!Files.exists(path)) {
      return Optional.empty();
    }
    try {
      byte[] bytes = Files.readAllBytes(path);
      touchFile(path);
      return Optional.of(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
    } catch (NoSuchFileException e) {
      return Optional.empty();
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  @Override
  public <E extends Exception> ByteBuffer get(
      K key, ThrowingFunction<? super K, ? extends ByteBuffer, E> mappingFunction) throws E {
    checkNotNull(key, "key cannot be null");
    checkNotNull(mappingFunction, "mappingFunction cannot be null");
    Optional<ByteBuffer> cached = get(key);
    if (cached.isPresent()) {
      return cached.get();
    }
    ByteBuffer computed = mappingFunction.apply(key);
    if (computed == null) {
      throw new NullPointerException("mappingFunction returned null for key: " + key);
    }
    put(key, computed);
    return computed.duplicate().asReadOnlyBuffer();
  }

  @Override
  public void put(K key, ByteBuffer value) {
    checkNotNull(key, "key cannot be null");
    checkNotNull(value, "value cannot be null");
    tryEvictIfNecessary();
    Path targetPath = getCacheFilePath(key);
    String tempFileName =
        TEMP_FILE_PREFIX
            + targetPath.getFileName().toString()
            + TEMP_FILE_MARKER
            + UUID.randomUUID();
    Path tempPath = cacheDirectory.resolve(tempFileName);
    try {
      ByteBuffer slice = value.duplicate();
      slice.rewind();
      byte[] bytes = new byte[slice.remaining()];
      slice.get(bytes);
      Files.write(tempPath, bytes);
      try {
        Files.move(
            tempPath,
            targetPath,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      try {
        Files.deleteIfExists(tempPath);
      } catch (IOException ignored) {
      }
    }
  }

  @Override
  public void invalidate(K key) {
    checkNotNull(key, "key cannot be null");
    Path path = getCacheFilePath(key);
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
    }
  }

  @Override
  public void invalidateAll() {
    try (Stream<Path> stream = Files.list(cacheDirectory)) {
      stream
          .filter(p -> p.getFileName().toString().endsWith(CACHE_FILE_EXTENSION))
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException ignored) {
                }
              });
    } catch (IOException ignored) {
    }
  }

  @Override
  public long size() {
    try (Stream<Path> stream = Files.list(cacheDirectory)) {
      return stream.filter(p -> p.getFileName().toString().endsWith(CACHE_FILE_EXTENSION)).count();
    } catch (IOException e) {
      return 0;
    }
  }

  @Override
  public void cleanUp() {
    tryEvictIfNecessary();
  }

  private AnalyticsCacheFileImpl(
      Path cacheDirectory,
      long maxSizeBytes,
      double highWatermarkRatio,
      double lowWatermarkRatio,
      long touchThrottleMillis,
      Function<K, String> keyToStringFunction) {
    checkNotNull(cacheDirectory, "cacheDirectory cannot be null");
    checkArgument(maxSizeBytes > 0, "maxSizeBytes must be positive");
    checkArgument(
        highWatermarkRatio > 0 && highWatermarkRatio <= 1.0,
        "highWatermarkRatio must be between 0 and 1");
    checkArgument(
        lowWatermarkRatio > 0 && lowWatermarkRatio < highWatermarkRatio,
        "lowWatermarkRatio must be positive and less than highWatermarkRatio");
    checkArgument(touchThrottleMillis >= 0, "touchThrottleMillis must be non-negative");
    checkNotNull(keyToStringFunction, "keyToStringFunction cannot be null");
    this.cacheDirectory = cacheDirectory;
    this.maxSizeBytes = maxSizeBytes;
    this.highWatermarkBytes = (long) (maxSizeBytes * highWatermarkRatio);
    this.lowWatermarkBytes = (long) (maxSizeBytes * lowWatermarkRatio);
    this.touchThrottleMillis = touchThrottleMillis;
    this.keyToStringFunction = keyToStringFunction;
    this.evictionLockPath = cacheDirectory.resolve(".eviction.lock");
    try {
      Files.createDirectories(cacheDirectory);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create cache directory: " + cacheDirectory, e);
    }
  }

  private Path getCacheFilePath(K key) {
    String keyStr = keyToStringFunction.apply(key);
    checkNotNull(keyStr, "key string representation cannot be null");
    String keyHash = Hashing.sha256().hashString(keyStr, StandardCharsets.UTF_8).toString();
    return cacheDirectory.resolve(keyHash + CACHE_FILE_EXTENSION);
  }

  private void touchFile(Path path) {
    try {
      FileTime lastModified = Files.getLastModifiedTime(path);
      long now = System.currentTimeMillis();
      if (now - lastModified.toMillis() > touchThrottleMillis) {
        Files.setLastModifiedTime(path, FileTime.fromMillis(now));
      }
    } catch (IOException ignored) {
    }
  }

  private void tryEvictIfNecessary() {
    try {
      long currentSize = calculateCacheSizeBytes();
      if (currentSize < highWatermarkBytes) {
        return;
      }
      try (FileChannel channel =
              FileChannel.open(
                  evictionLockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
          FileLock lock = channel.tryLock()) {
        if (lock != null) {
          performEviction();
        }
      }
    } catch (IOException ignored) {
    }
  }

  private long calculateCacheSizeBytes() throws IOException {
    try (Stream<Path> stream = Files.list(cacheDirectory)) {
      return stream
          .filter(p -> p.getFileName().toString().endsWith(CACHE_FILE_EXTENSION))
          .mapToLong(
              p -> {
                try {
                  return Files.size(p);
                } catch (IOException e) {
                  return 0L;
                }
              })
          .sum();
    }
  }

  private void performEviction() throws IOException {
    List<Path> cacheFiles;
    try (Stream<Path> stream = Files.list(cacheDirectory)) {
      cacheFiles =
          stream
              .filter(p -> p.getFileName().toString().endsWith(CACHE_FILE_EXTENSION))
              .collect(Collectors.toList());
    }

    List<CacheFileEntry> entries = new ArrayList<>();
    long totalBytes = 0;
    for (Path p : cacheFiles) {
      try {
        long size = Files.size(p);
        long mtime = Files.getLastModifiedTime(p).toMillis();
        entries.add(new CacheFileEntry(p, size, mtime));
        totalBytes += size;
      } catch (IOException ignored) {
      }
    }

    if (totalBytes >= highWatermarkBytes) {
      entries.sort(Comparator.comparingLong(e -> e.lastModified));
      for (CacheFileEntry entry : entries) {
        if (totalBytes <= lowWatermarkBytes) {
          break;
        }
        try {
          if (Files.deleteIfExists(entry.path)) {
            totalBytes -= entry.size;
          }
        } catch (IOException ignored) {
        }
      }
    }

    long staleThreshold = System.currentTimeMillis() - STALE_TEMP_FILE_THRESHOLD_MILLIS;
    try (Stream<Path> stream = Files.list(cacheDirectory)) {
      stream
          .filter(p -> p.getFileName().toString().contains(TEMP_FILE_MARKER))
          .forEach(
              p -> {
                try {
                  if (Files.getLastModifiedTime(p).toMillis() < staleThreshold) {
                    Files.deleteIfExists(p);
                  }
                } catch (IOException ignored) {
                }
              });
    }
  }

  private static class CacheFileEntry {
    private final Path path;
    private final long size;
    private final long lastModified;

    private CacheFileEntry(Path path, long size, long lastModified) {
      this.path = path;
      this.size = size;
      this.lastModified = lastModified;
    }
  }
}
