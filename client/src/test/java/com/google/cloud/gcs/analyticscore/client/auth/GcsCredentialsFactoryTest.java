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

package com.google.cloud.gcs.analyticscore.client.auth;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GcsCredentialsFactoryTest {

  private static final String SERVICE_ACCOUNT_JSON_TEMPLATE =
      "{\"type\":\"service_account\","
          + "\"project_id\":\"test-project\","
          + "\"private_key_id\":\"test-key-id\","
          + "\"private_key\":\"%s\","
          + "\"client_email\":\"test-email@gserviceaccount.com\","
          + "\"client_id\":\"test-client-id\","
          + "\"token_uri\":\"https://accounts.google.com/o/oauth2/token\"}";

  @TempDir static Path sharedTempDir;

  private static String serviceAccountKeyFile;

  private final HttpTransportFactory fakeTransportFactory = NetHttpTransport::new;

  @BeforeAll
  static void writeServiceAccountKeyFile() throws GeneralSecurityException, IOException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    PrivateKey privateKey = keyPairGenerator.generateKeyPair().getPrivate();
    String keyFileJson = String.format(SERVICE_ACCOUNT_JSON_TEMPLATE, toEscapedPem(privateKey));
    Path keyFile = sharedTempDir.resolve("service-account.json");
    Files.write(keyFile, keyFileJson.getBytes(UTF_8));
    serviceAccountKeyFile = keyFile.toString();
  }

  private static String toEscapedPem(PrivateKey privateKey) {
    String base64 =
        Base64.getMimeEncoder(64, "\n".getBytes(UTF_8)).encodeToString(privateKey.getEncoded());
    return ("-----BEGIN PRIVATE KEY-----\n" + base64 + "\n-----END PRIVATE KEY-----\n")
        .replace("\n", "\\n");
  }

  @Test
  void createCredentials_unauthenticated_returnsNoCredentials() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
  }

  @Test
  void createCredentials_computeEngine_returnsComputeEngineCredentials() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.COMPUTE_ENGINE).build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
  }

  @Test
  void createCredentials_computeEngineWithTokenServerUri_ignoresTokenServerUri()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.COMPUTE_ENGINE)
            .setTokenServerUri(URI.create("https://custom-auth.example.com/token"))
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ComputeEngineCredentials.class);
  }

  @Test
  void createCredentials_serviceAccountKeyfile_returnsServiceAccountCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(serviceAccountKeyFile)
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    ServiceAccountCredentials saCreds = (ServiceAccountCredentials) credentials;
    assertThat(saCreds.getClientEmail()).isEqualTo("test-email@gserviceaccount.com");
    assertThat(saCreds.getPrivateKeyId()).isEqualTo("test-key-id");
  }

  @Test
  void createCredentials_serviceAccountKeyfileWithTokenServerUri_usesCustomTokenServerUri()
      throws IOException {
    URI customUri = URI.create("https://custom-auth.example.com/token");
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(serviceAccountKeyFile)
            .setTokenServerUri(customUri)
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
    ServiceAccountCredentials saCreds = (ServiceAccountCredentials) credentials;
    assertThat(saCreds.toBuilder().getTokenServerUri()).isEqualTo(customUri);
  }

  @Test
  void createCredentials_impersonationServiceAccountSet_returnsImpersonatedCredentials()
      throws IOException {
    String targetSa = "target-impersonated@test-project.iam.gserviceaccount.com";
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(serviceAccountKeyFile)
            .setImpersonationServiceAccount(targetSa)
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ImpersonatedCredentials.class);
    ImpersonatedCredentials impCreds = (ImpersonatedCredentials) credentials;
    assertThat(impCreds.getAccount()).isEqualTo(targetSa);
  }

  @Test
  void createCredentials_impersonationServiceAccountBlank_returnsSourceCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile(serviceAccountKeyFile)
            .setImpersonationServiceAccount("   ")
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ServiceAccountCredentials.class);
  }

  @Test
  void createCredentials_unauthenticatedWithImpersonation_throwsIllegalArgumentException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.UNAUTHENTICATED)
            .setImpersonationServiceAccount("target@test-project.iam.gserviceaccount.com")
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> GcsCredentialsFactory.createCredentials(options, fakeTransportFactory));

    assertThat(exception)
        .hasMessageThat()
        .contains("impersonationServiceAccount cannot be set for UNAUTHENTICATED auth type");
  }

  @Test
  void createCredentials_unauthenticatedWithBlankImpersonation_returnsNoCredentials()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.UNAUTHENTICATED)
            .setImpersonationServiceAccount("   ")
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
  }

  @Test
  void createCredentials_serviceAccountKeyfile_fileNotFound_throwsIOException() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile("/non/existent/file.json")
            .build();

    assertThrows(
        FileNotFoundException.class,
        () -> GcsCredentialsFactory.createCredentials(options, fakeTransportFactory));
  }

  @Test
  void createCredentials_workloadIdentityFederation_returnsExternalAccountCredentials()
      throws IOException {
    String wipPath = getResourcePath("test-wip-config.json");
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.WORKLOAD_IDENTITY_FEDERATION)
            .setWorkloadIdentityCredentialConfigFile(wipPath)
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(ExternalAccountCredentials.class);
    ExternalAccountCredentials wifCreds = (ExternalAccountCredentials) credentials;
    assertThat(wifCreds.getAuthenticationType()).isEqualTo("OAuth2");
    assertThat(wifCreds.getAudience())
        .isEqualTo(
            "//iam.googleapis.com/projects/test/locations/global/workloadIdentityPools/test-pool/providers/tester");
  }

  @Test
  void createCredentials_userCredentials_returnsUserCredentials() throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("test-client-id")
            .setClientSecret(RedactedString.create("test-client-secret"))
            .setRefreshToken(RedactedString.create("test-refresh-token"))
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(UserCredentials.class);
    UserCredentials userCreds = (UserCredentials) credentials;
    assertThat(userCreds.getClientId()).isEqualTo("test-client-id");
    assertThat(userCreds.getClientSecret()).isEqualTo("test-client-secret");
    assertThat(userCreds.getRefreshToken()).isEqualTo("test-refresh-token");
  }

  @Test
  void createCredentials_userCredentialsWithTokenServerUri_usesCustomTokenServerUri()
      throws IOException {
    URI customUri = URI.create("https://custom-token.example.com");
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.USER_CREDENTIALS)
            .setClientId("test-client-id")
            .setClientSecret(RedactedString.create("test-client-secret"))
            .setRefreshToken(RedactedString.create("test-refresh-token"))
            .setTokenServerUri(customUri)
            .build();

    Credentials credentials =
        GcsCredentialsFactory.createCredentials(options, fakeTransportFactory);

    assertThat(credentials).isInstanceOf(UserCredentials.class);
    UserCredentials userCreds = (UserCredentials) credentials;
    assertThat(userCreds.toBuilder().getTokenServerUri()).isEqualTo(customUri);
  }

  @Test
  void createCredentials_usingDefaultTransport_unauthenticated() throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().setAuthType(AuthType.UNAUTHENTICATED).build();

    Credentials credentials = GcsCredentialsFactory.createCredentials(options);

    assertThat(credentials).isSameInstanceAs(NoCredentials.getInstance());
  }

  @Test
  void createCredentials_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsCredentialsFactory.createCredentials(null));
  }

  private String getResourcePath(String resourceName) {
    URL resource = getClass().getClassLoader().getResource(resourceName);
    assertThat(resource).isNotNull();
    try {
      return Paths.get(Objects.requireNonNull(resource).toURI()).toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
