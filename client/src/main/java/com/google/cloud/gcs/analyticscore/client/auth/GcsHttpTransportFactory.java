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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNullElseGet;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory for creating {@link HttpTransport} instances configured with proxies and timeouts. */
final class GcsHttpTransportFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GcsHttpTransportFactory.class);

  private GcsHttpTransportFactory() {}

  /**
   * Creates an {@link HttpTransport} configured according to the provided {@link GcsAuthOptions}.
   *
   * @param options The authentication options containing proxy and timeout configurations.
   * @param trustStore The trust store to validate the endpoint's TLS certificates against.
   * @return A configured {@link HttpTransport} instance.
   * @throws IOException If there is an issue establishing SSL trust or creating the transport.
   */
  static HttpTransport createHttpTransport(GcsAuthOptions options, CertificateTrustStore trustStore)
      throws IOException {
    return createHttpTransport(
        options.getProxyAddress().orElse(null),
        options.getProxyUsername().orElse(null),
        options.getProxyPassword().orElse(null),
        options.getHttpReadTimeout(),
        trustStore);
  }

  /**
   * Creates an {@link HttpTransport} based on optional HTTP proxy and socket read timeout.
   *
   * <p>The proxy is always contacted as an HTTP proxy: a scheme in {@code proxyAddress} is
   * validated but otherwise ignored, so {@code https://} does not imply TLS to the proxy itself.
   *
   * <p><b>Side effects:</b> supplying proxy credentials mutates process wide JDK state, as
   * described on {@link #createNetHttpTransport}.
   *
   * @param proxyAddress The HTTP proxy address of the form {@code [https?://]hostname:port}.
   * @param proxyUsername The HTTP proxy username.
   * @param proxyPassword The HTTP proxy password.
   * @param readTimeout The socket read timeout to apply to HTTP requests, which must be positive if
   *     set.
   * @param trustStore The trust store to validate the endpoint's TLS certificates against.
   * @return The resulting {@link HttpTransport}.
   * @throws IllegalArgumentException If proxy configuration parameters are invalid.
   * @throws IOException If an error occurs initializing the certificate trust store or transport.
   */
  static HttpTransport createHttpTransport(
      @Nullable String proxyAddress,
      @Nullable RedactedString proxyUsername,
      @Nullable RedactedString proxyPassword,
      @Nullable Duration readTimeout,
      CertificateTrustStore trustStore)
      throws IOException {
    LOG.debug(
        "createHttpTransport(proxyAddress={}, proxyAuthenticated={}, readTimeout={},"
            + " trustStore={})",
        proxyAddress,
        proxyUsername != null,
        readTimeout,
        trustStore);
    checkArgument(
        proxyAddress != null || (proxyUsername == null && proxyPassword == null),
        "if proxyAddress is null then proxyUsername and proxyPassword should be null too");
    checkArgument(
        (proxyUsername == null) == (proxyPassword == null),
        "both proxyUsername and proxyPassword should be null or not null together");

    URI proxyUri = parseProxyAddress(proxyAddress);
    try {
      PasswordAuthentication proxyAuth =
          proxyUsername == null
              ? null
              : new PasswordAuthentication(
                  proxyUsername.value(), proxyPassword.value().toCharArray());
      return createNetHttpTransport(proxyUri, proxyAuth, readTimeout, trustStore);
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to create NetHttpTransport with SSL trust store", e);
    }
  }

  /**
   * Creates a {@link NetHttpTransport} routed through the given proxy.
   *
   * <p><b>Side effects:</b> the JDK resolves proxy credentials through process wide state, so when
   * {@code proxyAuth} is set this replaces the JVM default {@link Authenticator} and clears the
   * {@code jdk.http.auth.tunneling.disabledSchemes} system property, re-enabling Basic
   * authentication for proxy tunnels. A later call with different credentials therefore wins for
   * every connection in the JVM, including connections opened by other libraries.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @param proxyAuth The proxy credentials, or {@code null} if the proxy needs no authentication.
   * @param readTimeout The socket read timeout, which must be positive if set.
   * @param trustStore The trust store to validate the endpoint's TLS certificates against.
   * @return The resulting {@link NetHttpTransport}.
   * @throws IllegalArgumentException If proxyAuth is set without a proxyUri.
   * @throws IOException If an error occurs initializing the certificate trust store.
   * @throws GeneralSecurityException If the certificate trust store cannot be loaded.
   */
  @VisibleForTesting
  static NetHttpTransport createNetHttpTransport(
      @Nullable URI proxyUri,
      @Nullable PasswordAuthentication proxyAuth,
      @Nullable Duration readTimeout,
      CertificateTrustStore trustStore)
      throws IOException, GeneralSecurityException {
    checkArgument(
        proxyUri != null || proxyAuth == null,
        "if proxyUri is null then proxyAuth should be null too");

    if (proxyAuth != null) {
      // Enable "Basic" authentication on JDK 8+
      System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
      Authenticator.setDefault(
          new Authenticator() {
            @Nullable
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
              // getRequestingHost() is null when the JDK only knows the proxy by address, so the
              // known proxy host is the receiver of the comparison.
              if (getRequestorType() == RequestorType.PROXY
                  && proxyUri.getHost().equalsIgnoreCase(getRequestingHost())
                  && getRequestingPort() == proxyUri.getPort()) {
                return proxyAuth;
              }
              return null;
            }
          });
    }
    return createNetHttpTransportBuilder(proxyUri, readTimeout, trustStore).build();
  }

  /**
   * Creates the {@link NetHttpTransport.Builder} that backs every transport this factory returns.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @param readTimeout The socket read timeout, which must be positive if set.
   * @param trustStore The trust store to validate the endpoint's TLS certificates against.
   * @return The configured builder.
   * @throws IOException If an error occurs initializing the certificate trust store.
   * @throws GeneralSecurityException If the certificate trust store cannot be loaded.
   */
  @VisibleForTesting
  static NetHttpTransport.Builder createNetHttpTransportBuilder(
      @Nullable URI proxyUri, @Nullable Duration readTimeout, CertificateTrustStore trustStore)
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
    if (trustStore == CertificateTrustStore.GOOGLE_BUNDLED) {
      builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
    }
    SSLSocketFactory wrappedSslSocketFactory =
        requireNonNullElseGet(
            builder.getSslSocketFactory(), HttpsURLConnection::getDefaultSSLSocketFactory);
    return builder
        .setSslSocketFactory(
            new CustomSslSocketFactory(wrappedSslSocketFactory, toReadTimeoutMillis(readTimeout)))
        .setProxy(createProxy(proxyUri));
  }

  /**
   * Creates the HTTP {@link Proxy} to route requests through.
   *
   * @param proxyUri The parsed proxy address, or {@code null} to connect directly.
   * @return The HTTP proxy, or {@code null} if {@code proxyUri} is null.
   */
  @VisibleForTesting
  @Nullable
  static Proxy createProxy(@Nullable URI proxyUri) {
    return proxyUri == null
        ? null
        : new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
  }

  /**
   * Converts a socket read timeout into the milliseconds accepted by {@link Socket#setSoTimeout}.
   *
   * <p>Both {@code null} and {@link Duration#ZERO} leave the read timeout unbounded, which is the
   * socket default. A timeout shorter than a millisecond is rejected rather than rounded down,
   * because rounding it down would silently leave reads unbounded.
   *
   * @param readTimeout The socket read timeout, which must not be negative.
   * @return The timeout in milliseconds, or {@code 0} to leave the read timeout unbounded.
   * @throws IllegalArgumentException If the timeout is negative, shorter than a millisecond, or
   *     overflows an int.
   */
  static int toReadTimeoutMillis(@Nullable Duration readTimeout) {
    return toTimeoutMillis(readTimeout, "readTimeout");
  }

  /**
   * Converts a timeout duration into milliseconds.
   *
   * <p>Both {@code null} and {@link Duration#ZERO} leave the timeout unbounded. A timeout shorter
   * than a millisecond is rejected rather than rounded down.
   *
   * @param timeout The timeout duration, which must not be negative.
   * @param timeoutName The name of the timeout parameter for error reporting.
   * @return The timeout in milliseconds, or {@code 0} to leave the timeout unbounded.
   * @throws IllegalArgumentException If the timeout is negative, shorter than a millisecond, or
   *     overflows an int.
   */
  static int toTimeoutMillis(@Nullable Duration timeout, String timeoutName) {
    if (timeout == null || timeout.isZero()) {
      return 0;
    }
    checkArgument(
        !timeout.isNegative(), "%s must not be negative, but was %s", timeoutName, timeout);
    long timeoutMillis = timeout.toMillis();
    checkArgument(
        timeoutMillis > 0, "%s must be at least 1 millisecond, but was %s", timeoutName, timeout);
    checkArgument(
        timeoutMillis <= Integer.MAX_VALUE,
        "%s must not exceed %s milliseconds, but was %s",
        timeoutName,
        Integer.MAX_VALUE,
        timeout);
    return (int) timeoutMillis;
  }

  /**
   * Parses an HTTP proxy address string into a {@link URI}.
   *
   * <p>A scheme is validated if present, but it does not change how the proxy is used: the proxy is
   * always contacted as an HTTP proxy, so {@code https://} does not imply TLS to the proxy itself.
   *
   * @param proxyAddress The address of the form {@code [https?://]HOST:PORT}.
   * @return The URI representation of the proxy, or {@code null} if proxyAddress is null or empty.
   * @throws IllegalArgumentException If the proxy address is malformed.
   */
  @VisibleForTesting
  @Nullable
  static URI parseProxyAddress(@Nullable String proxyAddress) {
    if (isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = (proxyAddress.contains("//") ? "" : "//") + proxyAddress;
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      checkArgument(
          isNullOrEmpty(scheme) || scheme.matches("https?"),
          "HTTP proxy address '%s' has invalid scheme '%s'.",
          proxyAddress,
          scheme);
      checkArgument(!isNullOrEmpty(host), "Proxy address '%s' has no host.", proxyAddress);
      checkArgument(port != -1, "Proxy address '%s' has no port.", proxyAddress);
      checkArgument(
          uri.equals(new URI(scheme, null, host, port, null, null, null)),
          "Invalid proxy address '%s'.",
          proxyAddress);
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
  }

  /** SSLSocketFactory wrapper to apply keep-alive and socket read timeout during TLS handshake. */
  @VisibleForTesting
  static final class CustomSslSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory wrappedSocketFactory;
    private final int readTimeoutMillis;

    /**
     * @param wrappedSocketFactory The socket factory to delegate socket creation to.
     * @param readTimeoutMillis The socket read timeout in milliseconds, or {@code 0} to leave the
     *     read timeout unbounded.
     */
    CustomSslSocketFactory(SSLSocketFactory wrappedSocketFactory, int readTimeoutMillis) {
      this.wrappedSocketFactory = wrappedSocketFactory;
      this.readTimeoutMillis = readTimeoutMillis;
    }

    /** Returns the socket factory that this factory delegates socket creation to. */
    @VisibleForTesting
    SSLSocketFactory getWrappedSocketFactory() {
      return wrappedSocketFactory;
    }

    /** Returns the read timeout applied to created sockets, where {@code 0} means unbounded. */
    @VisibleForTesting
    int getReadTimeoutMillis() {
      return readTimeoutMillis;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return wrappedSocketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return wrappedSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket() throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket());
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose)
        throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(s, consumed, autoClose));
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose)
        throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(s, host, port, autoClose));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port) throws IOException {
      return customizeSocket(wrappedSocketFactory.createSocket(address, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(host, port, clientAddress, clientPort));
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort)
        throws IOException {
      return customizeSocket(
          wrappedSocketFactory.createSocket(address, port, clientAddress, clientPort));
    }

    private Socket customizeSocket(Socket socket) throws IOException {
      try {
        // Enable TCP keep-alive.
        socket.setKeepAlive(true);

        // Set socket read timeout. This shouldn't be necessary, because we generally set the
        // timeout through other layers, such as
        // com.google.api.client.http.HttpRequest#setReadTimeout(int). However, setting it here
        // guarantees that the timeout is enforced during TLS handshake when using Conscrypt as the
        // security provider. (See discussion in https://github.com/google/conscrypt/issues/864 .)
        // A timeout of zero leaves the read timeout unbounded, which is the socket default.
        socket.setSoTimeout(readTimeoutMillis);
      } catch (IOException | RuntimeException e) {
        // The socket is owned by this method until it is returned, so a half configured socket must
        // not outlive the failure that abandoned it.
        try {
          socket.close();
        } catch (IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
        throw e;
      }
      return socket;
    }
  }
}
