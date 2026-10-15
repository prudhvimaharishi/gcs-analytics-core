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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Supplier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference-counted holder for a cached {@link GcsClientImpl} and its associated read thread pool
 * supplier.
 */
final class LeasedClientEntry {

  private final GcsClientImpl client;
  private final Supplier<ExecutorService> readExecutorServiceSupplier;
  private final AtomicInteger activeLeases;
  private volatile long lastReleasedNanos;
  private final long tokenExpiresAtMillis;
  private boolean closed = false;
  private boolean evictOnZeroLeases = false;

  LeasedClientEntry(
      GcsClientImpl client,
      Supplier<ExecutorService> readExecutorServiceSupplier,
      long nowNanos,
      long tokenExpiresAtMillis) {
    this.client = checkNotNull(client, "client should not be null");
    this.readExecutorServiceSupplier =
        checkNotNull(readExecutorServiceSupplier, "readExecutorServiceSupplier should not be null");
    this.activeLeases = new AtomicInteger(1);
    this.lastReleasedNanos = nowNanos;
    this.tokenExpiresAtMillis = tokenExpiresAtMillis;
  }

  GcsClientImpl client() {
    return client;
  }

  Supplier<ExecutorService> readExecutorServiceSupplier() {
    return readExecutorServiceSupplier;
  }

  int activeLeases() {
    return activeLeases.get();
  }

  long lastReleasedNanos() {
    return lastReleasedNanos;
  }

  long tokenExpiresAtMillis() {
    return tokenExpiresAtMillis;
  }

  /**
   * Atomically increments the active lease count if this entry is open, not marked for immediate
   * eviction, and its static token (if any) has not expired.
   */
  synchronized boolean tryRetain(long nowMillis) {
    if (closed || evictOnZeroLeases || nowMillis >= tokenExpiresAtMillis) {
      return false;
    }
    activeLeases.incrementAndGet();
    return true;
  }

  /**
   * Atomically decrements the active lease count and records {@code nowNanos} when the count
   * reaches zero.
   */
  synchronized int release(long nowNanos) {
    int current = activeLeases.get();
    if (current <= 0) {
      return 0;
    }
    int remaining = activeLeases.decrementAndGet();
    if (remaining == 0) {
      lastReleasedNanos = nowNanos;
    }
    return remaining;
  }

  synchronized void markEvictOnZeroLeases() {
    this.evictOnZeroLeases = true;
  }

  synchronized boolean shouldEvictImmediatelyOnRelease(long nowMillis) {
    return evictOnZeroLeases || nowMillis >= tokenExpiresAtMillis;
  }

  /**
   * Returns whether this entry has zero active leases and has either exceeded {@code idleTtlNanos}
   * or passed {@code tokenExpiresAtMillis}.
   */
  synchronized boolean isEvictable(long nowNanos, long idleTtlNanos, long nowMillis) {
    if (closed || activeLeases.get() != 0) {
      return false;
    }
    return (nowNanos - lastReleasedNanos >= idleTtlNanos) || (nowMillis >= tokenExpiresAtMillis);
  }

  /**
   * Atomically transitions this entry to {@code closed} if it currently has zero active leases,
   * preventing any concurrent {@link #tryRetain(long)} before resources are shut down.
   */
  synchronized boolean tryMarkClosedIfIdle() {
    if (closed || activeLeases.get() != 0) {
      return false;
    }
    closed = true;
    return true;
  }

  void closeClientAndExecutor() {
    GcsFileSystemImpl.shutdownExecutorService(readExecutorServiceSupplier.get());
    client.close();
  }
}
