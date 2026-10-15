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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.auth.GcsCredentialsFactory;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * JVM-wide reference-counted cache of {@link GcsClientImpl} instances keyed by {@link
 * ClientCacheKey}.
 *
 * <p>Entries with active leases ({@code activeLeases > 0}) are pinned and never evicted. Unleased
 * entries ({@code activeLeases == 0}) are evicted once {@code clientIdleTtl} or static token
 * expiration has elapsed, or when unleased entries exceed {@code maxIdleEntries}.
 */
final class GcsClientCache {

  static final Duration DEFAULT_CLIENT_IDLE_TTL = Duration.ofMinutes(10);
  static final int DEFAULT_MAX_IDLE_ENTRIES = 64;

  static final class Lease {
    private final ClientCacheKey key;
    private final LeasedClientEntry entry;

    Lease(ClientCacheKey key, LeasedClientEntry entry) {
      this.key = checkNotNull(key, "key should not be null");
      this.entry = checkNotNull(entry, "entry should not be null");
    }

    ClientCacheKey key() {
      return key;
    }

    LeasedClientEntry entry() {
      return entry;
    }

    GcsClientImpl client() {
      return entry.client();
    }

    Supplier<ExecutorService> readExecutorServiceSupplier() {
      return entry.readExecutorServiceSupplier();
    }
  }

  private static final GcsClientCache INSTANCE =
      new GcsClientCache(
          DEFAULT_CLIENT_IDLE_TTL,
          DEFAULT_MAX_IDLE_ENTRIES,
          Ticker.systemTicker(),
          System::currentTimeMillis);

  private final ConcurrentHashMap<ClientCacheKey, LeasedClientEntry> entries =
      new ConcurrentHashMap<>();
  private final long clientIdleTtlNanos;
  private final int maxIdleEntries;
  private final Ticker ticker;
  private final LongSupplier clockMillis;

  static GcsClientCache getInstance() {
    return INSTANCE;
  }

  @VisibleForTesting
  GcsClientCache(Duration clientIdleTtl, int maxIdleEntries, Ticker ticker) {
    this(clientIdleTtl, maxIdleEntries, ticker, createTickerBackedClockMillis(ticker));
  }

  @VisibleForTesting
  GcsClientCache(
      Duration clientIdleTtl, int maxIdleEntries, Ticker ticker, LongSupplier clockMillis) {
    checkNotNull(clientIdleTtl, "clientIdleTtl should not be null");
    checkArgument(!clientIdleTtl.isNegative(), "clientIdleTtl must not be negative");
    checkArgument(maxIdleEntries >= 0, "maxIdleEntries must not be negative");
    this.clientIdleTtlNanos = clientIdleTtl.toNanos();
    this.maxIdleEntries = maxIdleEntries;
    this.ticker = checkNotNull(ticker, "ticker should not be null");
    this.clockMillis = checkNotNull(clockMillis, "clockMillis should not be null");
  }

  private static LongSupplier createTickerBackedClockMillis(Ticker ticker) {
    checkNotNull(ticker, "ticker should not be null");
    long baseMillis = System.currentTimeMillis();
    long baseNanos = ticker.read();
    return () -> baseMillis + TimeUnit.NANOSECONDS.toMillis(ticker.read() - baseNanos);
  }

  Lease acquire(
      GcsFileSystemOptions fileSystemOptions,
      Supplier<Supplier<ExecutorService>> readExecutorSupplierFactory,
      Telemetry telemetry) {
    checkNotNull(fileSystemOptions, "fileSystemOptions should not be null");
    CredentialKey credentialKey =
        CredentialKey.fromAuthOptions(fileSystemOptions.getGcsAuthOptions());
    return acquireInternal(
        credentialKey,
        () -> createCredentials(fileSystemOptions),
        fileSystemOptions,
        readExecutorSupplierFactory,
        telemetry);
  }

  Lease acquire(
      Credentials credentials,
      GcsFileSystemOptions fileSystemOptions,
      Supplier<Supplier<ExecutorService>> readExecutorSupplierFactory,
      Telemetry telemetry) {
    checkNotNull(credentials, "credentials should not be null");
    checkNotNull(fileSystemOptions, "fileSystemOptions should not be null");
    CredentialKey credentialKey = CredentialKey.fromCredentials(credentials);
    return acquireInternal(
        credentialKey,
        () -> credentials,
        fileSystemOptions,
        readExecutorSupplierFactory,
        telemetry);
  }

  private Lease acquireInternal(
      CredentialKey credentialKey,
      Supplier<Credentials> credentialsSupplier,
      GcsFileSystemOptions fileSystemOptions,
      Supplier<Supplier<ExecutorService>> readExecutorSupplierFactory,
      Telemetry telemetry) {
    evictExpiredEntries();

    ClientCacheKey cacheKey = ClientCacheKey.create(credentialKey, fileSystemOptions);
    long nowNanos = ticker.read();
    long nowMillis = clockMillis.getAsLong();

    LeasedClientEntry acquiredEntry =
        entries.compute(
            cacheKey,
            (key, existing) -> {
              if (existing != null) {
                if (existing.tryRetain(nowMillis)) {
                  return existing;
                }
                existing.markEvictOnZeroLeases();
              }

              Credentials resolvedCredentials = credentialsSupplier.get();
              Supplier<ExecutorService> readExecutorSupplier = readExecutorSupplierFactory.get();
              GcsClientImpl newClient =
                  new GcsClientImpl(
                      resolvedCredentials,
                      fileSystemOptions.getGcsClientOptions(),
                      readExecutorSupplier,
                      telemetry,
                      fileSystemOptions.getGcsAuthOptions());
              long tokenExpiresAtMillis =
                  credentialKey.expirationTimeMillis().orElse(Long.MAX_VALUE);
              LeasedClientEntry newEntry =
                  new LeasedClientEntry(
                      newClient, readExecutorSupplier, nowNanos, tokenExpiresAtMillis);
              if (credentialKey.kind() == CredentialKey.Kind.UNIQUE_INSTANCE
                  || newClient.storage == null) {
                newEntry.markEvictOnZeroLeases();
              }
              return newEntry;
            });

    return new Lease(cacheKey, acquiredEntry);
  }

  void release(Lease lease) {
    checkNotNull(lease, "lease should not be null");
    long nowNanos = ticker.read();
    long nowMillis = clockMillis.getAsLong();
    LeasedClientEntry entry = lease.entry();
    int remaining = entry.release(nowNanos);
    if (remaining == 0) {
      if (entry.shouldEvictImmediatelyOnRelease(nowMillis)) {
        evictSpecificEntryIfIdle(lease.key(), entry);
      }
      evictExpiredEntries();
      evictExcessIdleEntries();
    }
  }

  void evictExpiredEntries() {
    long nowNanos = ticker.read();
    long nowMillis = clockMillis.getAsLong();

    for (Map.Entry<ClientCacheKey, LeasedClientEntry> mapEntry : entries.entrySet()) {
      LeasedClientEntry candidate = mapEntry.getValue();
      if (candidate.isEvictable(nowNanos, clientIdleTtlNanos, nowMillis)) {
        evictSpecificEntryIfIdle(mapEntry.getKey(), candidate);
      }
    }
  }

  private void evictExcessIdleEntries() {
    List<Map.Entry<ClientCacheKey, LeasedClientEntry>> idleEntries = new ArrayList<>();
    for (Map.Entry<ClientCacheKey, LeasedClientEntry> mapEntry : entries.entrySet()) {
      if (mapEntry.getValue().activeLeases() == 0) {
        idleEntries.add(mapEntry);
      }
    }
    int excessCount = idleEntries.size() - maxIdleEntries;
    if (excessCount <= 0) {
      return;
    }

    idleEntries.sort(Comparator.comparingLong(e -> e.getValue().lastReleasedNanos()));
    for (int i = 0; i < excessCount; i++) {
      Map.Entry<ClientCacheKey, LeasedClientEntry> victim = idleEntries.get(i);
      evictSpecificEntryIfIdle(victim.getKey(), victim.getValue());
    }
  }

  private void evictSpecificEntryIfIdle(ClientCacheKey key, LeasedClientEntry entry) {
    if (entry.tryMarkClosedIfIdle()) {
      entries.remove(key, entry);
      entry.closeClientAndExecutor();
    }
  }

  void clear() {
    for (Map.Entry<ClientCacheKey, LeasedClientEntry> mapEntry : entries.entrySet()) {
      LeasedClientEntry entry = mapEntry.getValue();
      entry.markEvictOnZeroLeases();
      evictSpecificEntryIfIdle(mapEntry.getKey(), entry);
    }
  }

  int size() {
    return entries.size();
  }

  private static Credentials createCredentials(GcsFileSystemOptions fileSystemOptions) {
    try {
      return GcsCredentialsFactory.createCredentials(fileSystemOptions.getGcsAuthOptions());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create credentials from auth options", e);
    }
  }
}
