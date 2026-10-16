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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.auth.AuthType;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CredentialKeyTest {

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final String READ_ONLY_SCOPE =
      "https://www.googleapis.com/auth/devstorage.read_only";
  private static final Date TOKEN_EXPIRATION = new Date(1_800_000_000_000L);

  @TempDir static Path sharedTempDir;

  private static PrivateKey privateKey1;
  private static PrivateKey privateKey2;
  private static Path serviceAccountKeyFile;
  private static Path wifConfigFile;

  @BeforeAll
  static void setUpKeysAndFiles() throws GeneralSecurityException, IOException {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    privateKey1 = generator.generateKeyPair().getPrivate();
    privateKey2 = generator.generateKeyPair().getPrivate();

    serviceAccountKeyFile = sharedTempDir.resolve("sa-key.json");
    Files.write(serviceAccountKeyFile, "{\"type\":\"service_account\"}".getBytes(UTF_8));

    wifConfigFile = sharedTempDir.resolve("wif-config.json");
    Files.write(
        wifConfigFile,
        Resources.toByteArray(Resources.getResource("test-workload-identity-config.json")));
  }

  @Test
  void fromCredentials_nullCredentials_returnsApplicationDefaultKind() {
    CredentialKey key = CredentialKey.fromCredentials(null);

    assertThat(key.kind()).isEqualTo(CredentialKey.Kind.APPLICATION_DEFAULT);
  }

  @Test
  void fromCredentials_noCredentials_returnsNoCredentialsKind() {
    CredentialKey key1 = CredentialKey.fromCredentials(NoCredentials.getInstance());
    CredentialKey key2 = CredentialKey.fromCredentials(NoCredentials.getInstance());

    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void fromCredentials_computeEngineCredentials_ignoresSyntheticTransportLambdaAndMatchesScopes() {
    ComputeEngineCredentials cred1 =
        ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(NetHttpTransport::new)
            .setScopes(ImmutableList.of(READ_ONLY_SCOPE, CLOUD_PLATFORM_SCOPE))
            .setDefaultScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .setQuotaProjectId("project-a")
            .setUniverseDomain("googleapis.com")
            .build();
    ComputeEngineCredentials cred2 =
        ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(() -> new NetHttpTransport())
            .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE, READ_ONLY_SCOPE))
            .setDefaultScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .setQuotaProjectId("project-a")
            .setUniverseDomain("googleapis.com")
            .build();

    assertThat(CredentialKey.fromCredentials(cred1))
        .isEqualTo(CredentialKey.fromCredentials(cred2));
  }

  @Test
  void fromCredentials_computeEngineCredentialsWithDifferentQuotaProject_producesDifferentKeys() {
    ComputeEngineCredentials cred1 =
        ComputeEngineCredentials.newBuilder().setQuotaProjectId("project-a").build();
    ComputeEngineCredentials cred2 =
        ComputeEngineCredentials.newBuilder().setQuotaProjectId("project-b").build();

    assertThat(CredentialKey.fromCredentials(cred1))
        .isNotEqualTo(CredentialKey.fromCredentials(cred2));
  }

  @Test
  void fromCredentials_serviceAccountCredentials_hashesPrivateKeyAndMatchesIdenticalConfig() {
    ServiceAccountCredentials cred1 =
        ServiceAccountCredentials.newBuilder()
            .setClientId("client-1")
            .setClientEmail("sa@test.iam.gserviceaccount.com")
            .setPrivateKey(privateKey1)
            .setPrivateKeyId("key-1")
            .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .setTokenServerUri(URI.create("https://oauth2.googleapis.com/token"))
            .setServiceAccountUser("delegated@example.com")
            .setQuotaProjectId("quota-proj")
            .setProjectId("proj-1")
            .setUniverseDomain("googleapis.com")
            .setHttpTransportFactory(NetHttpTransport::new)
            .build();
    ServiceAccountCredentials cred2 =
        ServiceAccountCredentials.newBuilder()
            .setClientId("client-1")
            .setClientEmail("sa@test.iam.gserviceaccount.com")
            .setPrivateKey(privateKey1)
            .setPrivateKeyId("key-1")
            .setScopes(ImmutableList.of(CLOUD_PLATFORM_SCOPE))
            .setTokenServerUri(URI.create("https://oauth2.googleapis.com/token"))
            .setServiceAccountUser("delegated@example.com")
            .setQuotaProjectId("quota-proj")
            .setProjectId("proj-1")
            .setUniverseDomain("googleapis.com")
            .setHttpTransportFactory(() -> new NetHttpTransport())
            .build();

    assertThat(CredentialKey.fromCredentials(cred1))
        .isEqualTo(CredentialKey.fromCredentials(cred2));
  }

  @Test
  void fromCredentials_serviceAccountCredentialsWithDifferentPrivateKey_producesDifferentKeys() {
    ServiceAccountCredentials cred1 =
        ServiceAccountCredentials.newBuilder()
            .setClientEmail("sa@test.iam.gserviceaccount.com")
            .setPrivateKey(privateKey1)
            .build();
    ServiceAccountCredentials cred2 =
        ServiceAccountCredentials.newBuilder()
            .setClientEmail("sa@test.iam.gserviceaccount.com")
            .setPrivateKey(privateKey2)
            .build();

    assertThat(CredentialKey.fromCredentials(cred1))
        .isNotEqualTo(CredentialKey.fromCredentials(cred2));
  }

  @Test
  void fromCredentials_serviceAccountCredentialsWithNullEncodedPrivateKey_handlesNullBytes() {
    PrivateKey unencodableKey =
        new PrivateKey() {
          @Override
          public String getAlgorithm() {
            return "RSA";
          }

          @Override
          public String getFormat() {
            return null;
          }

          @Override
          public byte[] getEncoded() {
            return null;
          }
        };
    ServiceAccountCredentials cred =
        ServiceAccountCredentials.newBuilder()
            .setClientEmail("sa@test.iam.gserviceaccount.com")
            .setPrivateKey(unencodableKey)
            .build();

    CredentialKey key = CredentialKey.fromCredentials(cred);

    assertThat(key.kind()).isEqualTo(CredentialKey.Kind.SERVICE_ACCOUNT);
  }

  @Test
  void fromCredentials_userCredentials_hashesSecretsAndIncludesTokenServerUri() {
    UserCredentials cred1 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("super-secret-value")
            .setRefreshToken("super-refresh-token")
            .setTokenServerUri(URI.create("https://custom.token.server/token"))
            .setQuotaProjectId("quota-1")
            .setHttpTransportFactory(NetHttpTransport::new)
            .build();
    UserCredentials cred2 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("super-secret-value")
            .setRefreshToken("super-refresh-token")
            .setTokenServerUri(URI.create("https://custom.token.server/token"))
            .setQuotaProjectId("quota-1")
            .setHttpTransportFactory(() -> new NetHttpTransport())
            .build();

    CredentialKey key1 = CredentialKey.fromCredentials(cred1);

    assertThat(
            ImmutableList.of(
                key1.equals(CredentialKey.fromCredentials(cred2)),
                key1.toString().contains("super-secret-value"),
                key1.toString().contains("super-refresh-token")))
        .containsExactly(true, false, false)
        .inOrder();
  }

  @Test
  void fromCredentials_userCredentialsWithDifferentTokenServerUri_producesDifferentKeys() {
    UserCredentials cred1 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("secret")
            .setRefreshToken("refresh")
            .setTokenServerUri(URI.create("https://token1.example.com/token"))
            .build();
    UserCredentials cred2 =
        UserCredentials.newBuilder()
            .setClientId("client-id")
            .setClientSecret("secret")
            .setRefreshToken("refresh")
            .setTokenServerUri(URI.create("https://token2.example.com/token"))
            .build();

    assertThat(CredentialKey.fromCredentials(cred1))
        .isNotEqualTo(CredentialKey.fromCredentials(cred2));
  }

  @Test
  void fromCredentials_accessTokenOnlyUserCredentialsForDifferentUsers_producesDifferentKeys() {
    UserCredentials alice = accessTokenOnlyUserCredentials("alice-access-token");
    UserCredentials bob = accessTokenOnlyUserCredentials("bob-access-token");

    CredentialKey aliceKey = CredentialKey.fromCredentials(alice);
    CredentialKey bobKey = CredentialKey.fromCredentials(bob);

    assertThat(aliceKey).isNotEqualTo(bobKey);
  }

  @Test
  void fromCredentials_accessTokenOnlyUserCredentialsWithSameToken_producesEqualKeys() {
    UserCredentials cred1 = accessTokenOnlyUserCredentials("alice-access-token");
    UserCredentials cred2 = accessTokenOnlyUserCredentials("alice-access-token");

    CredentialKey key1 = CredentialKey.fromCredentials(cred1);
    CredentialKey key2 = CredentialKey.fromCredentials(cred2);

    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void fromCredentials_accessTokenOnlyUserCredentials_expiresWithAccessToken() {
    UserCredentials cred = accessTokenOnlyUserCredentials("alice-access-token");

    CredentialKey key = CredentialKey.fromCredentials(cred);

    assertThat(key.expirationTimeMillis().getAsLong()).isEqualTo(TOKEN_EXPIRATION.getTime());
  }

  @Test
  void fromCredentials_accessTokenOnlyUserCredentials_doesNotExposeRawAccessToken() {
    UserCredentials cred = accessTokenOnlyUserCredentials("alice-access-token");

    CredentialKey key = CredentialKey.fromCredentials(cred);

    assertThat(key.toString()).doesNotContain("alice-access-token");
  }

  @Test
  void fromCredentials_userCredentialsWithRefreshToken_ignoresDifferingAccessTokens() {
    UserCredentials cred1 = refreshTokenUserCredentials("first-access-token");
    UserCredentials cred2 = refreshTokenUserCredentials("second-access-token");

    CredentialKey key1 = CredentialKey.fromCredentials(cred1);
    CredentialKey key2 = CredentialKey.fromCredentials(cred2);

    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void fromCredentials_userCredentialsWithoutRefreshOrAccessToken_failsClosedAsUniqueInstance() {
    UserCredentials cred =
        UserCredentials.newBuilder()
            .setClientId("gcloud-client-id")
            .setClientSecret("gcloud-client-secret")
            .setRefreshToken("")
            .build();

    CredentialKey key = CredentialKey.fromCredentials(cred);

    assertThat(key.kind()).isEqualTo(CredentialKey.Kind.UNIQUE_INSTANCE);
  }

  @Test
  void
      fromCredentials_impersonatedAccessTokenOnlyUserSourcesForDifferentUsers_producesDifferentKeys() {
    ImpersonatedCredentials aliceImpersonated =
        ImpersonatedCredentials.create(
            accessTokenOnlyUserCredentials("alice-access-token"),
            "target@test.iam.gserviceaccount.com",
            null,
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);
    ImpersonatedCredentials bobImpersonated =
        ImpersonatedCredentials.create(
            accessTokenOnlyUserCredentials("bob-access-token"),
            "target@test.iam.gserviceaccount.com",
            null,
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);

    CredentialKey aliceKey = CredentialKey.fromCredentials(aliceImpersonated);
    CredentialKey bobKey = CredentialKey.fromCredentials(bobImpersonated);

    assertThat(aliceKey).isNotEqualTo(bobKey);
  }

  @Test
  void fromCredentials_impersonatedCredentials_capturesSourceAndTargetAndDelegates() {
    ServiceAccountCredentials source =
        ServiceAccountCredentials.newBuilder()
            .setClientEmail("source@test.iam.gserviceaccount.com")
            .setPrivateKey(privateKey1)
            .build();
    ImpersonatedCredentials imp1 =
        ImpersonatedCredentials.create(
            source,
            "target@test.iam.gserviceaccount.com",
            ImmutableList.of("delegate-1"),
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);
    ImpersonatedCredentials imp2 =
        ImpersonatedCredentials.create(
            source,
            "target@test.iam.gserviceaccount.com",
            ImmutableList.of("delegate-1"),
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);
    ImpersonatedCredentials impDifferentTarget =
        ImpersonatedCredentials.create(
            source,
            "other@test.iam.gserviceaccount.com",
            null,
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);

    CredentialKey key1 = CredentialKey.fromCredentials(imp1);

    assertThat(
            ImmutableList.of(
                key1.equals(CredentialKey.fromCredentials(imp2)),
                key1.equals(CredentialKey.fromCredentials(impDifferentTarget))))
        .containsExactly(true, false)
        .inOrder();
  }

  @Test
  void fromCredentials_impersonatedCredentialsWithUnknownSource_failsClosedAsUniqueInstance() {
    GoogleCredentials unknownSource = new GoogleCredentials() {};
    ImpersonatedCredentials imp1 =
        ImpersonatedCredentials.create(
            unknownSource,
            "target@test.iam.gserviceaccount.com",
            null,
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);
    ImpersonatedCredentials imp2 =
        ImpersonatedCredentials.create(
            unknownSource,
            "target@test.iam.gserviceaccount.com",
            null,
            ImmutableList.of(CLOUD_PLATFORM_SCOPE),
            3600);

    CredentialKey key1 = CredentialKey.fromCredentials(imp1);
    CredentialKey key2 = CredentialKey.fromCredentials(imp2);

    assertThat(ImmutableList.of(key1.kind(), key1.equals(key2)))
        .containsExactly(CredentialKey.Kind.UNIQUE_INSTANCE, false)
        .inOrder();
  }

  @Test
  void fromCredentials_oauth2Credentials_hashesTokenAndExtractsExpiration() {
    Date expiration = new Date(1_800_000_000_000L);
    OAuth2Credentials cred1 =
        OAuth2Credentials.create(new AccessToken("raw-vended-token-123", expiration));
    OAuth2Credentials cred2 =
        OAuth2Credentials.create(new AccessToken("raw-vended-token-123", expiration));
    OAuth2Credentials credDifferentToken =
        OAuth2Credentials.create(new AccessToken("raw-vended-token-456", expiration));

    CredentialKey key1 = CredentialKey.fromCredentials(cred1);

    assertThat(
            ImmutableList.of(
                key1.equals(CredentialKey.fromCredentials(cred2)),
                key1.equals(CredentialKey.fromCredentials(credDifferentToken)),
                key1.expirationTimeMillis().getAsLong(),
                key1.toString().contains("raw-vended-token-123")))
        .containsExactly(true, false, 1_800_000_000_000L, false)
        .inOrder();
  }

  @Test
  void fromCredentials_oauth2CredentialsWithRefresh_staysInvariantAcrossInPlaceTokenRefresh()
      throws IOException {
    AtomicInteger refreshCounter = new AtomicInteger(0);
    AccessToken initialToken = new AccessToken("initial-token", new Date(1_800_000_000_000L));
    OAuth2CredentialsWithRefresh cred =
        OAuth2CredentialsWithRefresh.newBuilder()
            .setAccessToken(initialToken)
            .setRefreshHandler(
                () ->
                    new AccessToken(
                        "refreshed-token-" + refreshCounter.incrementAndGet(),
                        new Date(1_900_000_000_000L)))
            .build();

    CredentialKey keyAtCreation = CredentialKey.fromCredentials(cred);
    int initialKeyHash = keyAtCreation.hashCode();
    cred.refresh();

    assertThat(keyAtCreation.hashCode()).isEqualTo(initialKeyHash);
  }

  @Test
  void fromCredentials_oauth2CredentialsWithoutExpirationOrNullToken_handlesBothBranches() {
    OAuth2Credentials tokenWithoutExpiry =
        OAuth2Credentials.create(new AccessToken("token-no-exp", null));
    OAuth2Credentials emptyCredentials = OAuth2Credentials.newBuilder().build();
    OAuth2Credentials blankTokenCredentials = OAuth2Credentials.create(new AccessToken("", null));

    CredentialKey keyNoExp = CredentialKey.fromCredentials(tokenWithoutExpiry);
    CredentialKey keyNullToken = CredentialKey.fromCredentials(emptyCredentials);
    CredentialKey keyBlankToken = CredentialKey.fromCredentials(blankTokenCredentials);

    assertThat(
            ImmutableList.of(
                keyNoExp.kind(),
                keyNoExp.expirationTimeMillis().isPresent(),
                keyNullToken.kind(),
                keyBlankToken.kind()))
        .containsExactly(
            CredentialKey.Kind.VENDED_OAUTH2,
            false,
            CredentialKey.Kind.UNIQUE_INSTANCE,
            CredentialKey.Kind.UNIQUE_INSTANCE)
        .inOrder();
  }

  @Test
  void fromCredentials_customCredentialsSubclass_failsClosedWithUniqueInstancePerCall() {
    Credentials custom1 =
        new GoogleCredentials() {
          @Override
          public Map<String, List<String>> getRequestMetadata(URI uri) {
            return Collections.emptyMap();
          }
        };
    Credentials custom2 =
        new GoogleCredentials() {
          @Override
          public Map<String, List<String>> getRequestMetadata(URI uri) {
            return Collections.emptyMap();
          }
        };

    CredentialKey key1 = CredentialKey.fromCredentials(custom1);
    CredentialKey key2 = CredentialKey.fromCredentials(custom2);

    assertThat(ImmutableList.of(key1.kind(), key1.equals(key2)))
        .containsExactly(CredentialKey.Kind.UNIQUE_INSTANCE, false)
        .inOrder();
  }

  @Test
  void fromAuthOptions_allSupportedAuthTypes_buildDeterministicKeysAndDetectFileUpdates()
      throws IOException {
    GcsAuthOptions unauth = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();
    GcsAuthOptions defaultUnset = GcsAuthOptions.builder().build();
    GcsAuthOptions explicitAdc =
        GcsAuthOptions.builder().setAuthType(AuthType.APPLICATION_DEFAULT).build();
    GcsAuthOptions gce = GcsAuthOptions.builder().setAuthType(AuthType.COMPUTE_ENGINE).build();
    GcsAuthOptions user =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("cid")
            .setClientSecret(RedactedString.create("csecret"))
            .setRefreshToken(RedactedString.create("rtoken"))
            .build();
    GcsAuthOptions wif =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION)
            .setWorkloadIdentityCredentialConfigFile(wifConfigFile.toString())
            .build();
    GcsAuthOptions saKeyfile =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(serviceAccountKeyFile.toString())
            .build();

    CredentialKey saKeyBeforeTouch = CredentialKey.fromAuthOptions(saKeyfile);
    Files.setLastModifiedTime(
        serviceAccountKeyFile,
        FileTime.fromMillis(Files.getLastModifiedTime(serviceAccountKeyFile).toMillis() + 5_000L));
    CredentialKey saKeyAfterTouch = CredentialKey.fromAuthOptions(saKeyfile);

    assertThat(
            Arrays.asList(
                CredentialKey.fromAuthOptions(unauth).kind(),
                CredentialKey.fromAuthOptions(defaultUnset)
                    .equals(CredentialKey.fromAuthOptions(explicitAdc)),
                CredentialKey.fromAuthOptions(gce).kind(),
                CredentialKey.fromAuthOptions(user).toString().contains("csecret"),
                CredentialKey.fromAuthOptions(wif).kind(),
                saKeyBeforeTouch.equals(saKeyAfterTouch)))
        .containsExactly(
            CredentialKey.Kind.NO_CREDENTIALS,
            false,
            CredentialKey.Kind.COMPUTE_ENGINE,
            false,
            CredentialKey.Kind.WORKLOAD_IDENTITY_FEDERATION,
            false)
        .inOrder();
  }

  @Test
  void fromAuthOptions_missingKeyfile_throwsUncheckedIOException() {
    GcsAuthOptions missingKeyfileOptions =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile("/non/existent/sa-key.json")
            .build();

    assertThrows(
        UncheckedIOException.class, () -> CredentialKey.fromAuthOptions(missingKeyfileOptions));
  }

  private static UserCredentials accessTokenOnlyUserCredentials(String accessToken) {
    return UserCredentials.newBuilder()
        .setClientId("gcloud-client-id")
        .setClientSecret("gcloud-client-secret")
        .setAccessToken(new AccessToken(accessToken, TOKEN_EXPIRATION))
        .build();
  }

  private static UserCredentials refreshTokenUserCredentials(String accessToken) {
    return UserCredentials.newBuilder()
        .setClientId("gcloud-client-id")
        .setClientSecret("gcloud-client-secret")
        .setRefreshToken("gcloud-refresh-token")
        .setAccessToken(new AccessToken(accessToken, TOKEN_EXPIRATION))
        .build();
  }
}
