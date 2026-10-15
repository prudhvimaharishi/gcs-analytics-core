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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.auth.AuthType;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class GcsClientCacheTest {

  private static final Telemetry TEST_TELEMETRY = new Telemetry(ImmutableList.of());

  private static final class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong(1_000_000_000L);

    void advance(Duration duration) {
      nanos.addAndGet(duration.toNanos());
    }

    @Override
    public long read() {
      return nanos.get();
    }
  }

  private static Supplier<ExecutorService> newExecutorSupplier() {
    return Suppliers.memoize(() -> Executors.newSingleThreadExecutor());
  }

  private static GcsFileSystemOptions optionsWithProject(String projectId) {
    return GcsFileSystemOptions.builder()
        .setGcsClientOptions(GcsClientOptions.builder().setProjectId(projectId).build())
        .setGcsAuthOptions(GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build())
        .build();
  }

  @Test
  void acquire_identicalOptionsAndCredentials_reusesClientAndIncrementsLeases() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    GcsFileSystemOptions options = optionsWithProject("proj-1");

    GcsClientCache.Lease lease1 =
        cache.acquire(options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    GcsClientCache.Lease lease2 =
        cache.acquire(options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);

    assertThat(
            ImmutableList.of(
                lease1.client() == lease2.client(),
                lease1.readExecutorServiceSupplier() == lease2.readExecutorServiceSupplier(),
                lease1.entry().activeLeases(),
                cache.size()))
        .containsExactly(true, true, 2, 1)
        .inOrder();
    cache.release(lease1);
    cache.release(lease2);
    cache.clear();
  }

  @Test
  void acquire_differentCredentialsOrProxyOrTimeouts_createsDistinctClients() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    GcsFileSystemOptions baseOptions = optionsWithProject("proj-1");
    GcsFileSystemOptions proxyOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(GcsClientOptions.builder().setProjectId("proj-1").build())
            .setGcsAuthOptions(
                GcsAuthOptions.builder()
                    .setAuthType(AuthType.UNAUTHENTICATED)
                    .setProxyAddress("localhost:8080")
                    .setProxyUsername(RedactedString.create("user"))
                    .setProxyPassword(RedactedString.create("pass"))
                    .build())
            .build();
    Credentials userCred =
        UserCredentials.newBuilder()
            .setClientId("cid")
            .setClientSecret("sec")
            .setRefreshToken("ref")
            .build();

    GcsClientCache.Lease leaseNoCred =
        cache.acquire(
            NoCredentials.getInstance(),
            baseOptions,
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    GcsClientCache.Lease leaseProxy =
        cache.acquire(
            NoCredentials.getInstance(),
            proxyOptions,
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    GcsClientCache.Lease leaseUser =
        cache.acquire(
            userCred, baseOptions, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);

    assertThat(
            ImmutableList.of(
                leaseNoCred.client() != leaseProxy.client(),
                leaseNoCred.client() != leaseUser.client(),
                cache.size()))
        .containsExactly(true, true, 3)
        .inOrder();
    cache.release(leaseNoCred);
    cache.release(leaseProxy);
    cache.release(leaseUser);
    cache.clear();
  }

  @Test
  void release_whileAnotherLeaseActive_pinsEntryAcrossIdleTtlAndEvictsAfterLastRelease() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    GcsFileSystemOptions options = optionsWithProject("proj-pin");

    GcsClientCache.Lease lease1 =
        cache.acquire(options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    GcsClientCache.Lease lease2 =
        cache.acquire(options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    ExecutorService sharedReadExecutor = lease1.readExecutorServiceSupplier().get();

    cache.release(lease1);
    ticker.advance(Duration.ofMinutes(30));
    cache.evictExpiredEntries();
    boolean activeWhileLease2Held = !sharedReadExecutor.isShutdown() && cache.size() == 1;

    cache.release(lease2);
    ticker.advance(Duration.ofMinutes(11));
    cache.evictExpiredEntries();

    assertThat(
            ImmutableList.of(activeWhileLease2Held, sharedReadExecutor.isShutdown(), cache.size()))
        .containsExactly(true, true, 0)
        .inOrder();
  }

  @Test
  void acquire_staticTokenExpiresWhileUnleased_evictsOldClientAndCreatesNewClient() {
    FakeTicker ticker = new FakeTicker();
    AtomicLong clockMillis = new AtomicLong(1_000_000L);
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker, clockMillis::get);
    GcsFileSystemOptions options = optionsWithProject("proj-token-exp");
    OAuth2Credentials vendedToken =
        OAuth2Credentials.create(new AccessToken("token-1", new Date(1_060_000L)));

    GcsClientCache.Lease lease1 =
        cache.acquire(
            vendedToken, options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    ExecutorService firstExecutor = lease1.readExecutorServiceSupplier().get();
    cache.release(lease1);

    clockMillis.set(1_060_001L);
    OAuth2Credentials refreshedVendedToken =
        OAuth2Credentials.create(new AccessToken("token-2", new Date(2_000_000L)));
    GcsClientCache.Lease lease2 =
        cache.acquire(
            refreshedVendedToken, options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);

    assertThat(
            ImmutableList.of(
                lease1.client() != lease2.client(), firstExecutor.isShutdown(), cache.size()))
        .containsExactly(true, true, 1)
        .inOrder();
    cache.release(lease2);
    cache.clear();
  }

  @Test
  void acquire_sameStaticTokenExpiresWhileStillLeased_retiresOldEntryOnReleaseAndGivesNewClient() {
    FakeTicker ticker = new FakeTicker();
    AtomicLong clockMillis = new AtomicLong(1_000_000L);
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker, clockMillis::get);
    GcsFileSystemOptions options = optionsWithProject("proj-token-active-exp");
    OAuth2Credentials vendedToken =
        OAuth2Credentials.create(new AccessToken("token-1", new Date(1_060_000L)));

    GcsClientCache.Lease lease1 =
        cache.acquire(
            vendedToken, options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    ExecutorService firstExecutor = lease1.readExecutorServiceSupplier().get();

    // Token expires while lease1 is still active (activeLeases == 1)
    clockMillis.set(1_060_001L);
    GcsClientCache.Lease lease2 =
        cache.acquire(
            vendedToken, options, GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    boolean firstStillRunningBeforeRelease = !firstExecutor.isShutdown();

    cache.release(lease1);
    boolean firstShutDownAfterRelease1 = firstExecutor.isShutdown();

    assertThat(
            ImmutableList.of(
                lease1.client() != lease2.client(),
                firstStillRunningBeforeRelease,
                firstShutDownAfterRelease1))
        .containsExactly(true, true, true)
        .inOrder();
    cache.release(lease2);
    cache.clear();
  }

  @Test
  void release_exceedingMaxIdleEntries_evictsLeastRecentlyReleasedIdleEntry() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 2, ticker);

    GcsClientCache.Lease lease1 =
        cache.acquire(
            optionsWithProject("p1"), GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    GcsClientCache.Lease lease2 =
        cache.acquire(
            optionsWithProject("p2"), GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    GcsClientCache.Lease lease3 =
        cache.acquire(
            optionsWithProject("p3"), GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);
    GcsClientCache.Lease activeLease4 =
        cache.acquire(
            optionsWithProject("p4"), GcsClientCacheTest::newExecutorSupplier, TEST_TELEMETRY);

    ExecutorService exec1 = lease1.readExecutorServiceSupplier().get();
    ExecutorService exec2 = lease2.readExecutorServiceSupplier().get();
    ExecutorService exec3 = lease3.readExecutorServiceSupplier().get();

    cache.release(lease1);
    ticker.advance(Duration.ofSeconds(1));
    cache.release(lease2);
    ticker.advance(Duration.ofSeconds(1));
    cache.release(lease3);

    // 3 idle entries released with maxIdleEntries=2 -> oldest (lease1) evicted,
    // lease2/lease3/activeLease4 kept
    assertThat(
            ImmutableList.of(
                exec1.isShutdown(), exec2.isShutdown(), exec3.isShutdown(), cache.size()))
        .containsExactly(true, false, false, 3)
        .inOrder();
    cache.release(activeLease4);
    cache.clear();
  }

  @Test
  void acquire_uniqueInstanceCredentials_evictsAndClosesImmediatelyOnRelease() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    Credentials unknownCredentials = new GoogleCredentials() {};

    GcsClientCache.Lease lease =
        cache.acquire(
            unknownCredentials,
            optionsWithProject("proj-unique"),
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    ExecutorService executor = lease.readExecutorServiceSupplier().get();
    cache.release(lease);

    assertThat(ImmutableList.of(executor.isShutdown(), cache.size()))
        .containsExactly(true, 0)
        .inOrder();
  }

  @Test
  void clear_withActiveAndIdleEntries_closesIdleImmediatelyAndMarksActiveForEvictionOnRelease() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);

    GcsClientCache.Lease idleLease =
        cache.acquire(
            optionsWithProject("proj-idle"),
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    GcsClientCache.Lease activeLease =
        cache.acquire(
            optionsWithProject("proj-active"),
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    ExecutorService idleExec = idleLease.readExecutorServiceSupplier().get();
    ExecutorService activeExec = activeLease.readExecutorServiceSupplier().get();

    cache.release(idleLease);
    cache.clear();
    boolean idleClosedOnClear = idleExec.isShutdown();
    boolean activeAliveDuringClear = !activeExec.isShutdown();

    cache.release(activeLease);

    assertThat(
            ImmutableList.of(
                idleClosedOnClear, activeAliveDuringClear, activeExec.isShutdown(), cache.size()))
        .containsExactly(true, true, true, 0)
        .inOrder();
  }

  @Test
  void leasedClientEntry_extraReleaseAndClosedRetainGuards_preserveInvariants() {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    GcsClientCache.Lease lease =
        cache.acquire(
            optionsWithProject("proj-guards"),
            GcsClientCacheTest::newExecutorSupplier,
            TEST_TELEMETRY);
    LeasedClientEntry entry = lease.entry();

    int remainingAfterFirstRelease = entry.release(ticker.read());
    int remainingAfterExtraRelease = entry.release(ticker.read());
    entry.markEvictOnZeroLeases();
    boolean retainedWhenEvictOnZeroLeases = entry.tryRetain(0L);
    boolean markedClosedFirstTime = entry.tryMarkClosedIfIdle();
    boolean markedClosedSecondTime = entry.tryMarkClosedIfIdle();
    boolean retainedAfterClose = entry.tryRetain(0L);
    boolean evictableAfterClose =
        entry.isEvictable(ticker.read() + TimeUnit.MINUTES.toNanos(20), 0L, Long.MAX_VALUE);

    assertThat(
            ImmutableList.of(
                remainingAfterFirstRelease,
                remainingAfterExtraRelease,
                retainedWhenEvictOnZeroLeases,
                markedClosedFirstTime,
                markedClosedSecondTime,
                retainedAfterClose,
                evictableAfterClose,
                entry.tokenExpiresAtMillis()))
        .containsExactly(0, 0, false, true, false, false, false, Long.MAX_VALUE)
        .inOrder();
    entry.closeClientAndExecutor();
    cache.clear();
  }

  @Test
  void acquire_whenAuthOptionsFailOnCacheMiss_throwsUncheckedIOException() throws IOException {
    FakeTicker ticker = new FakeTicker();
    GcsClientCache cache = new GcsClientCache(Duration.ofMinutes(10), 64, ticker);
    Path malformedJsonFile = Files.createTempFile("malformed-sa-key", ".json");
    try {
      Files.writeString(malformedJsonFile, "{not-valid-json", StandardCharsets.UTF_8);
      GcsFileSystemOptions malformedKeyfileOptions =
          GcsFileSystemOptions.builder()
              .setGcsClientOptions(GcsClientOptions.builder().setProjectId("proj").build())
              .setGcsAuthOptions(
                  GcsAuthOptions.builder()
                      .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
                      .setServiceAccountJsonKeyfile(malformedJsonFile.toString())
                      .build())
              .build();

      assertThrows(
          UncheckedIOException.class,
          () ->
              cache.acquire(
                  malformedKeyfileOptions,
                  GcsClientCacheTest::newExecutorSupplier,
                  TEST_TELEMETRY));
    } finally {
      Files.deleteIfExists(malformedJsonFile);
    }
  }

  @Test
  void constructor_withNegativeTtlOrNegativeMaxIdleEntries_throwsIllegalArgumentException() {
    FakeTicker ticker = new FakeTicker();

    IllegalArgumentException negativeTtl =
        assertThrows(
            IllegalArgumentException.class,
            () -> new GcsClientCache(Duration.ofSeconds(-1), 64, ticker));
    IllegalArgumentException negativeMaxIdle =
        assertThrows(
            IllegalArgumentException.class,
            () -> new GcsClientCache(Duration.ofMinutes(10), -1, ticker));

    assertThat(
            ImmutableList.of(
                negativeTtl.getMessage().contains("clientIdleTtl"),
                negativeMaxIdle.getMessage().contains("maxIdleEntries")))
        .containsExactly(true, true)
        .inOrder();
  }
}
