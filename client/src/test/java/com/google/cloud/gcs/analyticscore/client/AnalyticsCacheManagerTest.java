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
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnalyticsCacheManagerTest {

  private static final String BUCKET_NAME = "test-bucket";
  private static final GcsItemId ITEM_ID =
      GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("o").build();
  private static final ByteBuffer FOOTER = ByteBuffer.wrap(new byte[] {1, 2, 3});

  private AnalyticsCacheManager manager;

  @BeforeEach
  void setUp() {
    AnalyticsCacheManager.resetCaches();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(true).build());
  }

  @Test
  void getFooter_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);

    ByteBuffer footer =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return FOOTER.duplicate();
            });
    ByteBuffer secondFooter =
        manager.getFooter(
            ITEM_ID,
            itemId -> {
              callCount.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {4, 5, 6});
            });

    assertThat(footer).isEqualTo(FOOTER);
    assertThat(callCount.get()).isEqualTo(1);
    assertThat(secondFooter).isEqualTo(FOOTER);
    assertThat(secondFooter).isNotSameInstanceAs(FOOTER);
    assertThat(secondFooter.isReadOnly()).isTrue();
  }

  @Test
  void getFooter_loaderThrowsIOException_rethrowsIOException() {
    assertThrows(
        IOException.class,
        () ->
            manager.getFooter(
                ITEM_ID,
                itemId -> {
                  throw new IOException("test-io-exception");
                }));
  }

  @Test
  void getFooter_cacheDisabled_anyKey_callsLoaderEveryTime() throws IOException {
    AnalyticsCacheManager.resetCaches();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());
    AtomicInteger callCount = new AtomicInteger(0);
    AnalyticsCacheManager.FooterLoader loader =
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        };

    manager.getFooter(ITEM_ID, loader);
    manager.getFooter(ITEM_ID, loader);

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void invalidateFooter_cacheDisabled_anyKey_succeeds() {
    AnalyticsCacheManager.resetCaches();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateFooter(ITEM_ID);
  }

  @Test
  void invalidateAll_cacheDisabled_anyKey_succeeds() {
    AnalyticsCacheManager.resetCaches();
    manager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(false).build());

    manager.invalidateAll();
  }

  @Test
  void invalidateSmallObject_present_removesEntry() throws IOException {
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .build();
    AnalyticsCacheManager.resetCaches();
    manager = new AnalyticsCacheManager(cacheOptions);
    manager.getSmallObject(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateSmallObject(ITEM_ID);

    AtomicInteger callCount = new AtomicInteger(0);
    manager.getSmallObject(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateFooter_present_removesEntry() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());

    manager.invalidateFooter(ITEM_ID);

    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void invalidateAll_withEntries_clearsCache() throws IOException {
    GcsItemId itemId2 = GcsItemId.builder().setBucketName(BUCKET_NAME).setObjectName("o2").build();
    manager.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());
    manager.getFooter(itemId2, itemId -> ByteBuffer.wrap(new byte[] {2}));
    manager.getBucketProperties(BUCKET_NAME, bucketName -> BucketProperties.create(true));
    AtomicInteger callCount = new AtomicInteger(0);

    manager.invalidateAll();

    manager.getFooter(
        ITEM_ID,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getFooter(
        itemId2,
        itemId -> {
          callCount.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getBucketProperties(
        BUCKET_NAME,
        bucketName -> {
          callCount.incrementAndGet();
          return BucketProperties.create(true);
        });
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void getBucketProperties_notPresent_computesAndCachesValue() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    BucketProperties bucketProperties = BucketProperties.create(true);

    BucketProperties result1 =
        manager.getBucketProperties(
            BUCKET_NAME,
            bucketName -> {
              callCount.incrementAndGet();
              return bucketProperties;
            });
    BucketProperties result2 =
        manager.getBucketProperties(
            BUCKET_NAME,
            bucketName -> {
              callCount.incrementAndGet();
              return BucketProperties.create(false);
            });

    assertThat(result1).isEqualTo(bucketProperties);
    assertThat(result2).isEqualTo(bucketProperties);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void getBucketProperties_loaderThrowsIOException_rethrowsIOException() {
    assertThrows(
        IOException.class,
        () ->
            manager.getBucketProperties(
                BUCKET_NAME,
                bucketName -> {
                  throw new IOException("test-io-exception");
                }));
  }

  @Test
  void invalidateBucketProperties_present_removesEntry() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    manager.getBucketProperties(BUCKET_NAME, bucketName -> BucketProperties.create(true));

    manager.invalidateBucketProperties(BUCKET_NAME);
    manager.getBucketProperties(
        BUCKET_NAME,
        bucketName -> {
          callCount.incrementAndGet();
          return BucketProperties.create(true);
        });

    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void getFooter_unauthorizedSecondManager_bypassesCacheAndThrowsException() throws IOException {
    AnalyticsCacheManager secondManager =
        new AnalyticsCacheManager(GcsCacheOptions.builder().setFooterCacheEnabled(true).build());
    GcsItemId tableFile =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file1.parquet")
            .build();
    manager.getFooter(tableFile, itemId -> FOOTER.duplicate());

    assertThrows(
        IOException.class,
        () ->
            secondManager.getFooter(
                tableFile,
                itemId -> {
                  throw new IOException("403 Forbidden");
                }));
  }

  @Test
  void getFooter_samePrefixDifferentFile_hitsCacheAfterFirstFileGranted() throws IOException {
    GcsItemId fileOne =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file1.parquet")
            .build();
    GcsItemId fileTwo =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file2.parquet")
            .build();
    AnalyticsCacheManager secondManager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder()
                .setFooterCacheEnabled(true)
                .setCacheScope(GcsCacheScope.EXECUTOR)
                .setUniformBucketLevelAccessEnabled(true)
                .build());
    AtomicInteger secondManagerCalls = new AtomicInteger(0);
    manager =
        new AnalyticsCacheManager(
            GcsCacheOptions.builder()
                .setFooterCacheEnabled(true)
                .setCacheScope(GcsCacheScope.EXECUTOR)
                .setUniformBucketLevelAccessEnabled(true)
                .build());
    manager.getFooter(fileOne, itemId -> FOOTER.duplicate());
    manager.getFooter(fileTwo, itemId -> FOOTER.duplicate());

    secondManager.getFooter(
        fileOne,
        itemId -> {
          secondManagerCalls.incrementAndGet();
          return FOOTER.duplicate();
        });
    ByteBuffer cachedResult =
        secondManager.getFooter(
            fileTwo,
            itemId -> {
              secondManagerCalls.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {9, 9, 9});
            });

    assertThat(secondManagerCalls.get()).isEqualTo(1);
    assertThat(cachedResult).isEqualTo(FOOTER);
  }

  @Test
  void getSmallObject_samePrefixDifferentFile_hitsCacheAfterFirstFileGranted() throws IOException {
    AnalyticsCacheManager.resetCaches();
    GcsItemId fileOne =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file1.parquet")
            .build();
    GcsItemId fileTwo =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file2.parquet")
            .build();
    GcsCacheOptions cacheOptions =
        GcsCacheOptions.builder()
            .setSmallObjectCacheEnabled(true)
            .setSmallObjectCacheMaxSizeBytes(200)
            .setCacheScope(GcsCacheScope.EXECUTOR)
            .setUniformBucketLevelAccessEnabled(true)
            .build();
    AnalyticsCacheManager secondManager = new AnalyticsCacheManager(cacheOptions);
    AtomicInteger secondManagerCalls = new AtomicInteger(0);
    manager = new AnalyticsCacheManager(cacheOptions);
    manager.getSmallObject(fileOne, itemId -> FOOTER.duplicate());
    manager.getSmallObject(fileTwo, itemId -> FOOTER.duplicate());

    secondManager.getSmallObject(
        fileOne,
        itemId -> {
          secondManagerCalls.incrementAndGet();
          return FOOTER.duplicate();
        });
    ByteBuffer cachedResult =
        secondManager.getSmallObject(
            fileTwo,
            itemId -> {
              secondManagerCalls.incrementAndGet();
              return ByteBuffer.wrap(new byte[] {9, 9, 9});
            });

    assertThat(secondManagerCalls.get()).isEqualTo(1);
    assertThat(cachedResult).isEqualTo(FOOTER);
  }

  @Test
  void getFooter_filesystemInstanceMode_isolatesBetweenManagers() throws IOException {
    AnalyticsCacheManager.resetCaches();
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(true)
            .setCacheScope(GcsCacheScope.INSTANCE)
            .build();
    AnalyticsCacheManager managerOne = new AnalyticsCacheManager(options);
    AnalyticsCacheManager managerTwo = new AnalyticsCacheManager(options);
    AtomicInteger managerTwoCalls = new AtomicInteger(0);

    managerOne.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());
    managerTwo.getFooter(
        ITEM_ID,
        itemId -> {
          managerTwoCalls.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(managerTwoCalls.get()).isEqualTo(1);
  }

  @Test
  void getFooter_executorAuthAwareMode_isolatesBetweenDifferentCredentials() throws IOException {
    AnalyticsCacheManager.resetCaches();
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(true)
            .setCacheScope(GcsCacheScope.EXECUTOR)
            .setUniformBucketLevelAccessEnabled(false)
            .build();
    AccessToken tokenOne = new AccessToken("token-one", null);
    AccessToken tokenTwo = new AccessToken("token-two", null);
    AnalyticsCacheManager managerOne =
        new AnalyticsCacheManager(OAuth2Credentials.create(tokenOne), options);
    AnalyticsCacheManager managerTwo =
        new AnalyticsCacheManager(OAuth2Credentials.create(tokenTwo), options);
    AtomicInteger managerTwoCalls = new AtomicInteger(0);

    managerOne.getFooter(ITEM_ID, itemId -> FOOTER.duplicate());
    managerTwo.getFooter(
        ITEM_ID,
        itemId -> {
          managerTwoCalls.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(managerTwoCalls.get()).isEqualTo(1);
  }

  @Test
  void getFooter_executorAuthAwareMode_bypassesPrefixShortcutForSecondFile() throws IOException {
    AnalyticsCacheManager.resetCaches();
    GcsCacheOptions options =
        GcsCacheOptions.builder()
            .setFooterCacheEnabled(true)
            .setCacheScope(GcsCacheScope.EXECUTOR)
            .setUniformBucketLevelAccessEnabled(false)
            .build();
    manager = new AnalyticsCacheManager(options);
    GcsItemId fileOne =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file1.parquet")
            .build();
    GcsItemId fileTwo =
        GcsItemId.builder()
            .setBucketName(BUCKET_NAME)
            .setObjectName("warehouse/table/file2.parquet")
            .build();
    AtomicInteger calls = new AtomicInteger(0);

    manager.getFooter(
        fileOne,
        itemId -> {
          calls.incrementAndGet();
          return FOOTER.duplicate();
        });
    manager.getFooter(
        fileTwo,
        itemId -> {
          calls.incrementAndGet();
          return FOOTER.duplicate();
        });

    assertThat(calls.get()).isEqualTo(2);
  }

  @Test
  void extractScope_nullCredentials_returnsAdc() {
    String scope = AnalyticsCacheManager.extractScope(null);

    assertThat(scope).isEqualTo("adc");
  }

  @Test
  void extractScope_noCredentials_returnsAnon() {
    String scope = AnalyticsCacheManager.extractScope(NoCredentials.getInstance());

    assertThat(scope).isEqualTo("anon");
  }

  @Test
  void extractScope_impersonatedCredentials_returnsImpAccount() {
    ImpersonatedCredentials credentials =
        ImpersonatedCredentials.newBuilder()
            .setSourceCredentials(GoogleCredentials.create(new AccessToken("source", null)))
            .setTargetPrincipal("target-sa@test.com")
            .setScopes(Collections.emptyList())
            .build();

    String scope = AnalyticsCacheManager.extractScope(credentials);

    assertThat(scope).isEqualTo("imp:target-sa@test.com");
  }

  @Test
  void extractScope_oauth2Credentials_returnsSha256OfToken() {
    AccessToken token = new AccessToken("secret-token", null);
    OAuth2Credentials credentials = OAuth2Credentials.create(token);

    String scope = AnalyticsCacheManager.extractScope(credentials);

    assertThat(scope).startsWith("tok:");
    assertThat(scope).isNotEqualTo("tok:secret-token");
  }

  @Test
  void extractScope_unknownCredentials_returnsCredWithHash() {
    Credentials credentials =
        new Credentials() {
          @Override
          public String getAuthenticationType() {
            return "test";
          }

          @Override
          public Map<String, List<String>> getRequestMetadata(URI uri) {
            return Collections.emptyMap();
          }

          @Override
          public boolean hasRequestMetadata() {
            return false;
          }

          @Override
          public boolean hasRequestMetadataOnly() {
            return false;
          }

          @Override
          public void refresh() {}
        };

    String scope = AnalyticsCacheManager.extractScope(credentials);

    assertThat(scope).startsWith("cred:");
  }
}
