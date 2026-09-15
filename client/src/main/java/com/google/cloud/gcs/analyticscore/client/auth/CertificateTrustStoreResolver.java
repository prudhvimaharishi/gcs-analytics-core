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

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.base.Ascii;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Chooses the certificate trust store that validates the TLS certificate presented by an endpoint.
 *
 * <p>Google's bundled trust store pins trust to the CAs that sign endpoints in the default {@code
 * googleapis.com} universe. An endpoint outside that universe, such as a Trusted Partner Cloud
 * deployment, is signed by a CA that is absent from the bundle and can only be validated against
 * the trust store the JVM was configured with.
 *
 * <p>The token server and the storage endpoint are resolved independently, because a deployment may
 * fetch tokens from Google while reading data from a custom endpoint, or the reverse.
 */
final class CertificateTrustStoreResolver {

  /** The universe domain served by the CAs in Google's bundled certificate trust store. */
  private static final String GOOGLE_DEFAULT_UNIVERSE_DOMAIN = "googleapis.com";

  private CertificateTrustStoreResolver() {}

  /**
   * Resolves the trust store that validates the token server's TLS certificate.
   *
   * @param tokenServerUri The configured token server URI, or empty to use the default token
   *     server, which always lives in the default Google universe.
   * @return The trust store to validate the token server against.
   */
  static CertificateTrustStore forTokenServer(Optional<URI> tokenServerUri) {
    return tokenServerUri.isPresent()
        ? resolveConfiguredEndpoint(Optional.ofNullable(tokenServerUri.get().getHost()))
        : CertificateTrustStore.GOOGLE_BUNDLED;
  }

  /**
   * Resolves the trust store that validates the storage endpoint's TLS certificate.
   *
   * @param storageEndpoint The configured storage endpoint, of the form {@code
   *     [scheme://]host[:port]}, or {@code null} to use the default endpoint.
   * @return The trust store to validate the storage endpoint against.
   */
  static CertificateTrustStore forStorageEndpoint(@Nullable String storageEndpoint) {
    return forStorageEndpoint(storageEndpoint, /* universeDomain= */ null);
  }

  /**
   * Resolves the trust store that validates the storage endpoint's TLS certificate, taking into
   * account both an explicit endpoint override and a configured universe domain.
   *
   * <p>An explicit storage endpoint takes precedence over a universe domain. When neither is set,
   * or when the resolved host/domain belongs to {@code googleapis.com}, Google's bundled trust
   * store is used. Otherwise, validation falls back to the JVM's default trust store.
   *
   * @param storageEndpoint The configured storage endpoint, or {@code null} if unset.
   * @param universeDomain The configured universe domain, or {@code null} if unset.
   * @return The trust store to validate the storage endpoint against.
   */
  static CertificateTrustStore forStorageEndpoint(
      @Nullable String storageEndpoint, @Nullable String universeDomain) {
    if (!isNullOrEmpty(storageEndpoint)) {
      return resolveConfiguredEndpoint(parseHost(storageEndpoint));
    }
    if (!isNullOrEmpty(universeDomain)) {
      return isGoogleDefaultUniverseHost(universeDomain.trim())
          ? CertificateTrustStore.GOOGLE_BUNDLED
          : CertificateTrustStore.SYSTEM_DEFAULT;
    }
    return CertificateTrustStore.GOOGLE_BUNDLED;
  }

  /**
   * Returns the trust store for an endpoint the user configured explicitly.
   *
   * <p>Only a host that is proven to live in the default Google universe keeps Google's pinned
   * bundle. An endpoint that cannot be parsed into a host falls back to the JVM trust store, which
   * still validates Google endpoints, whereas wrongly pinning a custom endpoint to Google's bundle
   * fails the handshake outright.
   *
   * @param host The host the endpoint resolves to, or empty if it cannot be determined.
   * @return The trust store to validate the endpoint against.
   */
  private static CertificateTrustStore resolveConfiguredEndpoint(Optional<String> host) {
    return host.filter(CertificateTrustStoreResolver::isGoogleDefaultUniverseHost).isPresent()
        ? CertificateTrustStore.GOOGLE_BUNDLED
        : CertificateTrustStore.SYSTEM_DEFAULT;
  }

  private static boolean isGoogleDefaultUniverseHost(String host) {
    String lowerCaseHost = Ascii.toLowerCase(host);
    return lowerCaseHost.equals(GOOGLE_DEFAULT_UNIVERSE_DOMAIN)
        || lowerCaseHost.endsWith("." + GOOGLE_DEFAULT_UNIVERSE_DOMAIN);
  }

  /**
   * Extracts the host from an endpoint that may omit the scheme.
   *
   * @return The host, or empty if the endpoint cannot be parsed into one.
   */
  private static Optional<String> parseHost(String endpoint) {
    String uriString = (endpoint.contains("//") ? "" : "//") + endpoint;
    try {
      return Optional.ofNullable(new URI(uriString).getHost());
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }
}
