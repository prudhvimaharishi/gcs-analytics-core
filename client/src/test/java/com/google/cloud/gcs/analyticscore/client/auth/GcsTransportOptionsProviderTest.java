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

import com.google.cloud.http.HttpTransportOptions;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class GcsTransportOptionsProviderTest {

  @Test
  void getStorageTransportOptions_forwardsConnectAndReadTimeouts() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setHttpConnectTimeout(Duration.ofSeconds(12))
            .setHttpReadTimeout(Duration.ofSeconds(34))
            .build();
    GcsTransportOptionsProvider provider = new GcsTransportOptionsProvider(options);

    HttpTransportOptions transportOptions = provider.getStorageTransportOptions();

    assertThat(transportOptions.getConnectTimeout()).isEqualTo(12_000);
    assertThat(transportOptions.getReadTimeout()).isEqualTo(34_000);
  }

  @Test
  void usesSystemDefaultTrustStore_defaultEndpoints_returnsFalse() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider = new GcsTransportOptionsProvider(options);

    assertThat(provider.usesSystemDefaultTrustStore()).isFalse();
  }

  @Test
  void usesSystemDefaultTrustStore_customStorageEndpoint_returnsTrue() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(
            options, "https://localhost:8443", /* universeDomain= */ null);

    assertThat(provider.usesSystemDefaultTrustStore()).isTrue();
  }

  /**
   * The token server is reached by the token transport, which is always pinned to Google's bundled
   * trust store, so it must not influence the data plane trust store.
   */
  @Test
  void usesSystemDefaultTrustStore_customTokenServerUri_returnsFalse() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setTokenServerUri(URI.create("https://sts.apis-tpclp.goog/token"))
            .build();
    GcsTransportOptionsProvider provider = new GcsTransportOptionsProvider(options);

    assertThat(provider.usesSystemDefaultTrustStore()).isFalse();
  }

  @Test
  void usesSystemDefaultTrustStore_customUniverseDomain_returnsTrue() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(options, /* storageEndpoint= */ null, "apis-tpclp.goog");

    assertThat(provider.usesSystemDefaultTrustStore()).isTrue();
  }

  @Test
  void usesSystemDefaultTrustStore_defaultGoogleUniverseDomain_returnsFalse() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(options, /* storageEndpoint= */ null, "googleapis.com");

    assertThat(provider.usesSystemDefaultTrustStore()).isFalse();
  }
}
