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
import static com.google.common.base.Strings.isNullOrEmpty;

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
 * <p>The two transports are kept separate because they trust different certificate authorities.
 * Tokens are always fetched from a Google endpoint, so the token transport is pinned to Google's
 * bundled trust store. The data plane may be pointed at a custom endpoint or a non-default universe
 * domain whose CA is not in that bundle, so its transport falls back to the JVM's default trust
 * store in that case.
 *
 * <p>Each transport is created lazily and memoized, so constructing this provider performs no I/O
 * and the token transport is never created when a caller supplies its own credentials.
 */
public final class GcsTransportOptionsProvider {

  private static final String GOOGLE_DEFAULT_UNIVERSE_DOMAIN = "googleapis.com";

  private final Supplier<HttpTransport> authTransport;
  private final Supplier<HttpTransport> storageTransport;
  private final int connectTimeoutMillis;
  private final int readTimeoutMillis;
  private final boolean hasProxyConfiguration;
  private final boolean useSystemDefaultTrustStore;

  /**
   * Creates a provider for a deployment that uses the default storage endpoint.
   *
   * @param options The authentication options supplying the proxy and timeouts.
   */
  public GcsTransportOptionsProvider(GcsAuthOptions options) {
    this(options, /* storageEndpoint= */ null, /* universeDomain= */ null);
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
    this.useSystemDefaultTrustStore = usesNonGoogleEndpoint(storageEndpoint, universeDomain);
    this.authTransport = memoizeTransport(options, /* useSystemDefaultTrustStore= */ false);
    this.storageTransport = memoizeTransport(options, this.useSystemDefaultTrustStore);
  }

  /**
   * Returns {@code true} when data plane requests target a custom endpoint or a non-default
   * universe domain whose TLS certificate may not be in Google's bundled trust store.
   */
  @VisibleForTesting
  static boolean usesNonGoogleEndpoint(
      @Nullable String storageEndpoint, @Nullable String universeDomain) {
    if (!isNullOrEmpty(storageEndpoint)) {
      return true;
    }
    return !isNullOrEmpty(universeDomain)
        && !GOOGLE_DEFAULT_UNIVERSE_DOMAIN.equalsIgnoreCase(universeDomain.trim());
  }

  private static Supplier<HttpTransport> memoizeTransport(
      GcsAuthOptions options, boolean useSystemDefaultTrustStore) {
    return Suppliers.memoize(
        () -> {
          try {
            return GcsHttpTransportFactory.createHttpTransport(options, useSystemDefaultTrustStore);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  /**
   * Returns the transport factory used to fetch OAuth2 tokens.
   *
   * @return The factory supplying the memoized token transport.
   */
  public com.google.auth.http.HttpTransportFactory getAuthTransportFactory() {
    return authTransport::get;
  }

  /**
   * Returns the transport options used by the GCS data plane storage client.
   *
   * @return The transport options wrapping the memoized data plane transport.
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

  /** Returns whether the data plane transport validates against the JVM's default trust store. */
  @VisibleForTesting
  boolean usesSystemDefaultTrustStore() {
    return useSystemDefaultTrustStore;
  }
}
