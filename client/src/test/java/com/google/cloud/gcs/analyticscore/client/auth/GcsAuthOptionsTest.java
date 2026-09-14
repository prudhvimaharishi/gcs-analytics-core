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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GcsAuthOptionsTest {

  @Test
  void builder_defaults_areSetCorrectly() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    assertThat(options.getAuthType()).isEqualTo(AuthType.APPLICATION_DEFAULT);
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(5));
    assertThat(options.getServiceAccountJsonKeyfile().isPresent()).isFalse();
    assertThat(options.getWorkloadIdentityPoolConfigFile().isPresent()).isFalse();
    assertThat(options.getClientId().isPresent()).isFalse();
    assertThat(options.getClientSecret().isPresent()).isFalse();
    assertThat(options.getRefreshToken().isPresent()).isFalse();
    assertThat(options.getImpersonationServiceAccount().isPresent()).isFalse();
    assertThat(options.getTokenServerUri().isPresent()).isFalse();
    assertThat(options.getProxyAddress().isPresent()).isFalse();
    assertThat(options.getProxyUsername().isPresent()).isFalse();
    assertThat(options.getProxyPassword().isPresent()).isFalse();
  }

  @Test
  void builder_customValues_areSetCorrectly() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE)
            .setServiceAccountJsonKeyfile("/path/to/key.json")
            .setWorkloadIdentityPoolConfigFile("/path/to/wip.json")
            .setClientId("client-123")
            .setClientSecret("secret-456")
            .setRefreshToken("token-789")
            .setImpersonationServiceAccount("sa@project.iam.gserviceaccount.com")
            .setTokenServerUri(URI.create("https://oauth2.googleapis.com/token"))
            .setProxyAddress("proxy.mycompany.com:8080")
            .setProxyUsername("proxyuser")
            .setProxyPassword("proxypass")
            .setHttpReadTimeout(Duration.ofSeconds(15))
            .build();

    assertThat(options.getAuthType()).isEqualTo(AuthType.SERVICE_ACCOUNT_JSON_KEYFILE);
    assertThat(options.getServiceAccountJsonKeyfile()).hasValue("/path/to/key.json");
    assertThat(options.getWorkloadIdentityPoolConfigFile()).hasValue("/path/to/wip.json");
    assertThat(options.getClientId()).hasValue("client-123");
    assertThat(options.getClientSecret()).hasValue("secret-456");
    assertThat(options.getRefreshToken()).hasValue("token-789");
    assertThat(options.getImpersonationServiceAccount())
        .hasValue("sa@project.iam.gserviceaccount.com");
    assertThat(options.getTokenServerUri())
        .hasValue(URI.create("https://oauth2.googleapis.com/token"));
    assertThat(options.getProxyAddress()).hasValue("proxy.mycompany.com:8080");
    assertThat(options.getProxyUsername()).hasValue("proxyuser");
    assertThat(options.getProxyPassword()).hasValue("proxypass");
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void toBuilder_modifiesFieldsCorrectly() {
    GcsAuthOptions original =
        GcsAuthOptions.builder()
            .setAuthType(AuthType.COMPUTE_ENGINE)
            .setProxyAddress("proxy:8080")
            .build();

    GcsAuthOptions modified =
        original.toBuilder().setAuthType(AuthType.UNAUTHENTICATED).setProxyAddress(null).build();

    assertThat(modified.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
    assertThat(modified.getProxyAddress().isPresent()).isFalse();
    assertThat(original.getAuthType()).isEqualTo(AuthType.COMPUTE_ENGINE);
    assertThat(original.getProxyAddress()).hasValue("proxy:8080");
  }

  @Test
  void createFromOptions_withDotPrefix_parsesAllFields() {
    Map<String, String> map = new HashMap<>();
    map.put("gcs.auth.type", "USER_CREDENTIALS");
    map.put("gcs.auth.service.account.json.keyfile", "/path/to/key.json");
    map.put("gcs.auth.workload.identity.federation.credential.config.file", "/path/to/wip.json");
    map.put("gcs.auth.client.id", "client-id");
    map.put("gcs.auth.client.secret", "client-secret");
    map.put("gcs.auth.refresh.token", "refresh-token");
    map.put("gcs.auth.impersonation.service.account", "target@iam.gserviceaccount.com");
    map.put("gcs.token.server.url", "https://oauth2.googleapis.com/token");
    map.put("gcs.proxy.address", "https://proxy:8443");
    map.put("gcs.proxy.username", "user");
    map.put("gcs.proxy.password", "pass");
    map.put("gcs.http.read-timeout", "10000");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "gcs.");

    assertThat(options.getAuthType()).isEqualTo(AuthType.USER_CREDENTIALS);
    assertThat(options.getServiceAccountJsonKeyfile()).hasValue("/path/to/key.json");
    assertThat(options.getWorkloadIdentityPoolConfigFile()).hasValue("/path/to/wip.json");
    assertThat(options.getClientId()).hasValue("client-id");
    assertThat(options.getClientSecret()).hasValue("client-secret");
    assertThat(options.getRefreshToken()).hasValue("refresh-token");
    assertThat(options.getImpersonationServiceAccount()).hasValue("target@iam.gserviceaccount.com");
    assertThat(options.getTokenServerUri())
        .hasValue(URI.create("https://oauth2.googleapis.com/token"));
    assertThat(options.getProxyAddress()).hasValue("https://proxy:8443");
    assertThat(options.getProxyUsername()).hasValue("user");
    assertThat(options.getProxyPassword()).hasValue("pass");
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void createFromOptions_withoutTrailingDot_normalizesPrefix() {
    Map<String, String> map = ImmutableMap.of("gcs.auth.type", "COMPUTE_ENGINE");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "gcs");

    assertThat(options.getAuthType()).isEqualTo(AuthType.COMPUTE_ENGINE);
  }

  @Test
  void createFromOptions_withEmptyPrefix_parsesUnprefixedKeys() {
    Map<String, String> map = ImmutableMap.of("auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
  }

  @Test
  void createFromOptions_withNullPrefix_parsesUnprefixedKeys() {
    Map<String, String> map = ImmutableMap.of("auth.type", "UNAUTHENTICATED");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, null);

    assertThat(options.getAuthType()).isEqualTo(AuthType.UNAUTHENTICATED);
  }

  @Test
  void createFromOptions_alternativeKeys_parsedCorrectly() {
    Map<String, String> map = new HashMap<>();
    map.put("auth.token.server.url", "https://alt-token.example.com");
    map.put("http.read.timeout", "30s");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getTokenServerUri()).hasValue(URI.create("https://alt-token.example.com"));
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void createFromOptions_blankValues_areIgnored() {
    Map<String, String> map = new HashMap<>();
    map.put("auth.type", "   ");
    map.put("auth.service.account.json.keyfile", "   ");
    map.put("auth.workload.identity.federation.credential.config.file", "   ");
    map.put("auth.client.id", "   ");
    map.put("auth.client.secret", "   ");
    map.put("auth.refresh.token", "   ");
    map.put("auth.impersonation.service.account", "   ");
    map.put("token.server.url", "   ");
    map.put("auth.token.server.url", "   ");
    map.put("proxy.address", "   ");
    map.put("proxy.username", "   ");
    map.put("proxy.password", "   ");
    map.put("http.read-timeout", "   ");
    map.put("http.read.timeout", "   ");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options).isEqualTo(GcsAuthOptions.builder().build());
  }

  @Test
  void createFromOptions_blankPrimaryKeys_fallsBackToAlternativeKeys() {
    Map<String, String> map = new HashMap<>();
    map.put("token.server.url", "   ");
    map.put("auth.token.server.url", "https://alt-token.example.com");
    map.put("http.read-timeout", "   ");
    map.put("http.read.timeout", "45s");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getTokenServerUri()).hasValue(URI.create("https://alt-token.example.com"));
    assertThat(options.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(45));
  }

  @Test
  void createFromOptions_valuesWithWhitespace_areTrimmed() {
    Map<String, String> map = ImmutableMap.of("auth.client.id", "  client-id  ");

    GcsAuthOptions options = GcsAuthOptions.createFromOptions(map, "");

    assertThat(options.getClientId()).hasValue("client-id");
  }

  @Test
  void createFromOptions_durationFormats_parsedCorrectly() {
    // Milliseconds suffix
    GcsAuthOptions optsMs =
        GcsAuthOptions.createFromOptions(ImmutableMap.of("http.read-timeout", "2500ms"), "");
    assertThat(optsMs.getHttpReadTimeout()).isEqualTo(Duration.ofMillis(2500));

    // Seconds suffix
    GcsAuthOptions optsS =
        GcsAuthOptions.createFromOptions(ImmutableMap.of("http.read-timeout", "10s"), "");
    assertThat(optsS.getHttpReadTimeout()).isEqualTo(Duration.ofSeconds(10));

    // Minutes suffix
    GcsAuthOptions optsM =
        GcsAuthOptions.createFromOptions(ImmutableMap.of("http.read-timeout", "2m"), "");
    assertThat(optsM.getHttpReadTimeout()).isEqualTo(Duration.ofMinutes(2));

    // No suffix, interpreted as milliseconds
    GcsAuthOptions optsPlain =
        GcsAuthOptions.createFromOptions(ImmutableMap.of("http.read-timeout", "1500"), "");
    assertThat(optsPlain.getHttpReadTimeout()).isEqualTo(Duration.ofMillis(1500));
  }

  @Test
  void createFromOptions_invalidDuration_throwsIllegalArgumentException() {
    Map<String, String> map = ImmutableMap.of("http.read-timeout", "not-a-duration");
    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nonNumericValueWithSecondsSuffix_throwsIllegalArgumentException() {
    Map<String, String> map = ImmutableMap.of("http.read-timeout", "abcs");

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nonNumericValueWithMillisSuffix_throwsIllegalArgumentException() {
    Map<String, String> map = ImmutableMap.of("http.read-timeout", "abcms");

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nonNumericValueWithMinutesSuffix_throwsIllegalArgumentException() {
    Map<String, String> map = ImmutableMap.of("http.read-timeout", "abcm");

    assertThrows(IllegalArgumentException.class, () -> GcsAuthOptions.createFromOptions(map, ""));
  }

  @Test
  void createFromOptions_nullOptions_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> GcsAuthOptions.createFromOptions(null, "gcs."));
  }
}
