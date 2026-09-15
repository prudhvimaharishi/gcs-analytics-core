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

import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CertificateTrustStoreResolverTest {

  @Test
  void forTokenServer_noTokenServerUri_usesGoogleBundledTrustStore() {
    assertThat(CertificateTrustStoreResolver.forTokenServer(Optional.empty()))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://oauth2.googleapis.com/token",
        "https://sts.googleapis.com/v1/token",
        "https://STS.GOOGLEAPIS.COM/v1/token"
      })
  void forTokenServer_googleTokenServerUri_usesGoogleBundledTrustStore(String tokenServerUri) {
    assertThat(
            CertificateTrustStoreResolver.forTokenServer(Optional.of(URI.create(tokenServerUri))))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://sts.my-universe.tpc.goog/v1/token",
        "https://token-server.example.com/token",
        "https://notgoogleapis.com/token"
      })
  void forTokenServer_nonGoogleTokenServerUri_usesSystemDefaultTrustStore(String tokenServerUri) {
    assertThat(
            CertificateTrustStoreResolver.forTokenServer(Optional.of(URI.create(tokenServerUri))))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @Test
  void forTokenServer_tokenServerUriWithoutHost_usesSystemDefaultTrustStore() {
    assertThat(CertificateTrustStoreResolver.forTokenServer(Optional.of(URI.create("/v1/token"))))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @Test
  void forStorageEndpoint_noStorageEndpoint_usesGoogleBundledTrustStore() {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(null))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @Test
  void forStorageEndpoint_emptyStorageEndpoint_usesGoogleBundledTrustStore() {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(""))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://storage.googleapis.com",
        "https://storage.googleapis.com:443",
        "https://STORAGE.GOOGLEAPIS.COM",
        "storage.googleapis.com",
        "googleapis.com"
      })
  void forStorageEndpoint_googleStorageEndpoint_usesGoogleBundledTrustStore(String endpoint) {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(endpoint))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "https://storage.my-universe.tpc.goog",
        "https://notgoogleapis.com",
        "http://localhost:9000",
        "storage.example.com:8443"
      })
  void forStorageEndpoint_customStorageEndpoint_usesSystemDefaultTrustStore(String endpoint) {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(endpoint))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @Test
  void forStorageEndpoint_unparseableStorageEndpoint_usesSystemDefaultTrustStore() {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint("https://[not-an-address"))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"googleapis.com", "GOOGLEAPIS.COM", "  googleapis.com  "})
  void forStorageEndpoint_googleDefaultUniverseDomain_usesGoogleBundledTrustStore(
      String universeDomain) {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(null, universeDomain))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }

  @ParameterizedTest
  @ValueSource(strings = {"apis-tpclp.goog", "my-universe.example.com", "notgoogleapis.com"})
  void forStorageEndpoint_customUniverseDomain_usesSystemDefaultTrustStore(String universeDomain) {
    assertThat(CertificateTrustStoreResolver.forStorageEndpoint(null, universeDomain))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @Test
  void forStorageEndpoint_explicitCustomEndpointOverridesDefaultUniverseDomain() {
    assertThat(
            CertificateTrustStoreResolver.forStorageEndpoint(
                "https://localhost:8443", "googleapis.com"))
        .isEqualTo(CertificateTrustStore.SYSTEM_DEFAULT);
  }

  @Test
  void forStorageEndpoint_explicitGoogleEndpointOverridesCustomUniverseDomain() {
    assertThat(
            CertificateTrustStoreResolver.forStorageEndpoint(
                "https://storage.googleapis.com", "apis-tpclp.goog"))
        .isEqualTo(CertificateTrustStore.GOOGLE_BUNDLED);
  }
}
