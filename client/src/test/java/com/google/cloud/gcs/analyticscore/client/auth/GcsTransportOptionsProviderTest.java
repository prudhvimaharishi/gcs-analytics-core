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
  void sharesSingleTransport_defaultEndpoints_returnsTrue() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider = new GcsTransportOptionsProvider(options);

    assertThat(provider.sharesSingleTransport()).isTrue();
  }

  @Test
  void sharesSingleTransport_customStorageEndpoint_returnsFalse() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(options, "https://localhost:8443");

    assertThat(provider.sharesSingleTransport()).isFalse();
  }

  @Test
  void sharesSingleTransport_customUniverseDomain_returnsFalse() {
    GcsAuthOptions options = GcsAuthOptions.builder().build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(options, /* storageEndpoint= */ null, "apis-tpclp.goog");

    assertThat(provider.sharesSingleTransport()).isFalse();
  }

  @Test
  void sharesSingleTransport_bothCustomTokenServerAndCustomUniverseDomain_returnsTrue() {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setTokenServerUri(URI.create("https://sts.apis-tpclp.goog/token"))
            .build();
    GcsTransportOptionsProvider provider =
        new GcsTransportOptionsProvider(options, /* storageEndpoint= */ null, "apis-tpclp.goog");

    assertThat(provider.sharesSingleTransport()).isTrue();
  }
}
