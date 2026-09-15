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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpTransport;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nullable;

/**
 * Supplies the HTTP transports used to fetch OAuth2 tokens and to reach the GCS data plane.
 *
 * <p>Both transports apply the same proxy and socket read timeout, so a deployment behind an HTTP
 * proxy reaches the token server and the storage endpoint alike. They differ only in the
 * certificate trust store, which each endpoint resolves for itself: tokens may come from Google
 * while data comes from a custom endpoint, or the reverse. When both resolve to the same trust
 * store, which is the common case, a single transport is shared between them.
 *
 * <p>Transports are created lazily and memoized, so constructing this provider performs no I/O.
 */
public final class GcsTransportOptionsProvider {

  private final Supplier<HttpTransport> authTransport;
  private final Supplier<HttpTransport> storageTransport;
  private final int connectTimeoutMillis;
  private final int readTimeoutMillis;
  private final boolean hasProxyConfiguration;

  /**
   * Creates a provider for a deployment that uses the default storage endpoint.
   *
   * @param options The authentication options supplying the proxy and timeouts.
   */
  public GcsTransportOptionsProvider(GcsAuthOptions options) {
    this(options, /* storageEndpoint= */ null, /* universeDomain= */ null);
  }

  /**
   * Creates a provider for a deployment that may use a custom storage endpoint.
   *
   * @param options The authentication options supplying the proxy and timeouts.
   * @param storageEndpoint The configured storage endpoint, of the form {@code
   *     [scheme://]host[:port]}, or {@code null} to use the default endpoint.
   */
  public GcsTransportOptionsProvider(GcsAuthOptions options, @Nullable String storageEndpoint) {
    this(options, storageEndpoint, /* universeDomain= */ null);
  }

  /**
   * Creates a provider for a deployment that may use a custom storage endpoint or universe domain.
   *
   * @param options The authentication options supplying the proxy and timeouts.
   * @param storageEndpoint The configured storage endpoint, of the form {@code
   *     [scheme://]host[:port]}, or {@code null} to use the default endpoint.
   * @param universeDomain The configured universe domain, or {@code null} to use the default
   *     universe domain.
   */
  public GcsTransportOptionsProvider(
      GcsAuthOptions options, @Nullable String storageEndpoint, @Nullable String universeDomain) {
    checkNotNull(options, "options cannot be null");
    this.hasProxyConfiguration = options.getProxyAddress().isPresent();
    this.connectTimeoutMillis =
        GcsHttpTransportFactory.toTimeoutMillis(options.getHttpConnectTimeout(), "connectTimeout");
    this.readTimeoutMillis =
        GcsHttpTransportFactory.toReadTimeoutMillis(options.getHttpReadTimeout());

    CertificateTrustStore authTrustStore =
        CertificateTrustStoreResolver.forTokenServer(options.getTokenServerUri());
    CertificateTrustStore storageTrustStore =
        CertificateTrustStoreResolver.forStorageEndpoint(storageEndpoint, universeDomain);

    this.authTransport = memoizeTransport(options, authTrustStore);
    // Only the trust store distinguishes the two transports, so reuse one when it matches rather
    // than holding a second connection pool open for the life of the client.
    this.storageTransport =
        storageTrustStore == authTrustStore
            ? this.authTransport
            : memoizeTransport(options, storageTrustStore);
  }

  private static Supplier<HttpTransport> memoizeTransport(
      GcsAuthOptions options, CertificateTrustStore trustStore) {
    return Suppliers.memoize(
        () -> {
          try {
            return GcsHttpTransportFactory.createHttpTransport(options, trustStore);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  /**
   * Returns the transport factory used to fetch OAuth2 tokens.
   *
   * <p>Its trust store follows the configured token server, because that is the only endpoint this
   * transport contacts.
   *
   * @return The factory supplying the memoized token transport.
   */
  public com.google.auth.http.HttpTransportFactory getAuthTransportFactory() {
    return authTransport::get;
  }

  /**
   * Returns the transport options used by the GCS data plane storage client.
   *
   * <p>Its trust store follows the configured storage endpoint and universe domain. The connect and
   * read timeouts are applied to the HTTP request in addition to the underlying socket.
   *
   * @return The transport options wrapping the memoized storage transport.
   */
  public HttpTransportOptions getStorageTransportOptions() {
    return HttpTransportOptions.newBuilder()
        .setHttpTransportFactory(storageTransport::get)
        .setConnectTimeout(connectTimeoutMillis)
        .setReadTimeout(readTimeoutMillis)
        .build();
  }

  /**
   * Returns whether an HTTP proxy is configured.
   *
   * @return {@code true} if requests are routed through a proxy, {@code false} otherwise.
   */
  public boolean hasProxyConfiguration() {
    return hasProxyConfiguration;
  }

  /** Returns whether the token and storage transports are backed by the same instance. */
  @VisibleForTesting
  boolean sharesSingleTransport() {
    return authTransport == storageTransport;
  }
}
