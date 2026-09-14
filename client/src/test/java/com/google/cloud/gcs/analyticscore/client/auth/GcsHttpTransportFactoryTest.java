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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.cloud.gcs.analyticscore.client.auth.GcsHttpTransportFactory.CustomSslSocketFactory;
import com.google.cloud.gcs.analyticscore.common.RedactedString;
import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.Socket;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Duration;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class GcsHttpTransportFactoryTest {

  private static final String TUNNELING_DISABLED_SCHEMES_PROPERTY =
      "jdk.http.auth.tunneling.disabledSchemes";
  private static final RedactedString PROXY_USERNAME = RedactedString.create("user");
  private static final RedactedString PROXY_PASSWORD = RedactedString.create("pass");

  private Authenticator originalAuthenticator;
  private String originalTunnelingDisabledSchemes;

  /**
   * Captures the JVM wide state that configuring an authenticated proxy overwrites, so that these
   * tests cannot leak a default {@link Authenticator} into tests that run after them.
   */
  @BeforeEach
  void captureGlobalState() {
    originalAuthenticator = Authenticator.getDefault();
    originalTunnelingDisabledSchemes = System.getProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
  }

  @AfterEach
  void restoreGlobalState() {
    Authenticator.setDefault(originalAuthenticator);
    if (originalTunnelingDisabledSchemes == null) {
      System.clearProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY);
    } else {
      System.setProperty(TUNNELING_DISABLED_SCHEMES_PROPERTY, originalTunnelingDisabledSchemes);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "proxy.example.com:8080, //proxy.example.com:8080",
    "http://proxy.example.com:8080, http://proxy.example.com:8080",
    "https://proxy.example.com:8443, https://proxy.example.com:8443",
    "127.0.0.1:3128, //127.0.0.1:3128"
  })
  void parseProxyAddress_validAddress_returnsExpectedUri(String input, String expected) {
    URI uri = GcsHttpTransportFactory.parseProxyAddress(input);

    assertThat(uri).isEqualTo(URI.create(expected));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void parseProxyAddress_nullOrEmpty_returnsNull(String input) {
    URI uri = GcsHttpTransportFactory.parseProxyAddress(input);

    assertThat(uri).isNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "ftp://proxy.example.com:21",
        "http://proxy.example.com",
        "proxy.example.com",
        "http://:8080",
        ":8080",
        "http://proxy.example.com:notaport",
        "   ",
        "  proxy.example.com:8080  "
      })
  void parseProxyAddress_invalidAddress_throwsIllegalArgumentException(String invalidInput) {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsHttpTransportFactory.parseProxyAddress(invalidInput));
  }

  @Test
  void createProxy_nullUri_returnsNull() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(null);

    assertThat(proxy).isNull();
  }

  @Test
  void createProxy_proxyUri_returnsHttpProxy() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(URI.create("//127.0.0.1:3128"));

    assertThat(proxy.type()).isEqualTo(Proxy.Type.HTTP);
  }

  @Test
  void createProxy_proxyUri_returnsProxyWithConfiguredAddress() {
    Proxy proxy = GcsHttpTransportFactory.createProxy(URI.create("//127.0.0.1:3128"));

    assertThat(proxy.address()).isEqualTo(new InetSocketAddress("127.0.0.1", 3128));
  }

  @Test
  void createNetHttpTransportBuilder_installsCustomSslSocketFactory()
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder =
        GcsHttpTransportFactory.createNetHttpTransportBuilder(null, Duration.ofSeconds(5));

    assertThat(builder.getSslSocketFactory()).isInstanceOf(CustomSslSocketFactory.class);
  }

  @Test
  void createHttpTransport_noProxy_createsNetHttpTransport() throws IOException {
    HttpTransport transport =
        GcsHttpTransportFactory.createHttpTransport(null, null, null, Duration.ofSeconds(5));

    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_withProxy_createsNetHttpTransport() throws IOException {
    HttpTransport transport =
        GcsHttpTransportFactory.createHttpTransport(
            "proxy.example.com:8080", PROXY_USERNAME, PROXY_PASSWORD, Duration.ofSeconds(10));

    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_withProxyCredentials_installsDefaultAuthenticator() throws IOException {
    GcsHttpTransportFactory.createHttpTransport(
        "proxy.example.com:8080", PROXY_USERNAME, PROXY_PASSWORD, Duration.ofSeconds(10));

    assertThat(Authenticator.getDefault()).isNotNull();
  }

  @Test
  void createHttpTransport_fromGcsAuthOptionsWithProxy_createsNetHttpTransport()
      throws IOException {
    GcsAuthOptions options =
        GcsAuthOptions.builder()
            .setProxyAddress("proxy.example.com:8080")
            .setProxyUsername(PROXY_USERNAME)
            .setProxyPassword(PROXY_PASSWORD)
            .setHttpReadTimeout(Duration.ofSeconds(10))
            .build();

    HttpTransport transport = GcsHttpTransportFactory.createHttpTransport(options);

    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_fromGcsAuthOptionsWithoutProxy_createsNetHttpTransport()
      throws IOException {
    GcsAuthOptions options = GcsAuthOptions.builder().build();

    HttpTransport transport = GcsHttpTransportFactory.createHttpTransport(options);

    assertThat(transport).isInstanceOf(NetHttpTransport.class);
  }

  @Test
  void createHttpTransport_proxyAuthWithoutProxyAddress_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, PROXY_USERNAME, PROXY_PASSWORD, null));
  }

  @Test
  void createHttpTransport_proxyUsernameWithoutPassword_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                "proxy.example.com:8080", PROXY_USERNAME, null, null));
  }

  @Test
  void createHttpTransport_proxyPasswordWithoutUsername_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                "proxy.example.com:8080", null, PROXY_PASSWORD, null));
  }

  @ParameterizedTest
  @ValueSource(longs = {0L, -1L})
  void createHttpTransport_nonPositiveReadTimeout_throwsIllegalArgumentException(long millis) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createHttpTransport(
                null, null, null, Duration.ofMillis(millis)));
  }

  @Test
  void createHttpTransport_readTimeoutOverflowingInt_throwsIllegalArgumentException() {
    Duration readTimeout = Duration.ofMillis(Integer.MAX_VALUE + 1L);

    assertThrows(
        IllegalArgumentException.class,
        () -> GcsHttpTransportFactory.createHttpTransport(null, null, null, readTimeout));
  }

  @Test
  void createNetHttpTransport_proxyAuthWithoutProxyUri_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GcsHttpTransportFactory.createNetHttpTransport(
                null, new PasswordAuthentication("u", "p".toCharArray()), null));
  }

  @Test
  void customSslSocketFactory_createSocket_enablesKeepAlive() throws IOException {
    CustomSslSocketFactory socketFactory =
        new CustomSslSocketFactory(new FakeSslSocketFactory(), 0);

    try (Socket socket = socketFactory.createSocket()) {
      assertThat(socket.getKeepAlive()).isTrue();
    }
  }

  @Test
  void customSslSocketFactory_createSocket_appliesReadTimeout() throws IOException {
    CustomSslSocketFactory socketFactory =
        new CustomSslSocketFactory(new FakeSslSocketFactory(), 1234);

    try (Socket socket = socketFactory.createSocket()) {
      assertThat(socket.getSoTimeout()).isEqualTo(1234);
    }
  }

  @Test
  void customSslSocketFactory_createSocket_zeroTimeoutLeavesReadTimeoutUnbounded()
      throws IOException {
    CustomSslSocketFactory socketFactory =
        new CustomSslSocketFactory(new FakeSslSocketFactory(), 0);

    try (Socket socket = socketFactory.createSocket()) {
      assertThat(socket.getSoTimeout()).isEqualTo(0);
    }
  }

  @Test
  void customSslSocketFactory_createSocketForHostAndPort_appliesReadTimeout() throws IOException {
    CustomSslSocketFactory socketFactory =
        new CustomSslSocketFactory(new FakeSslSocketFactory(), 4321);

    try (Socket socket = socketFactory.createSocket("host.example.com", 443)) {
      assertThat(socket.getSoTimeout()).isEqualTo(4321);
    }
  }

  @Test
  void customSslSocketFactory_createSocketLayeredOverSocket_appliesReadTimeout()
      throws IOException {
    CustomSslSocketFactory socketFactory =
        new CustomSslSocketFactory(new FakeSslSocketFactory(), 4321);

    try (Socket underlying = new Socket();
        Socket socket = socketFactory.createSocket(underlying, "host.example.com", 443, false)) {
      assertThat(socket.getSoTimeout()).isEqualTo(4321);
    }
  }

  @Test
  void customSslSocketFactory_getDefaultCipherSuites_delegatesToWrappedFactory() {
    FakeSslSocketFactory wrapped = new FakeSslSocketFactory();
    CustomSslSocketFactory socketFactory = new CustomSslSocketFactory(wrapped, 0);

    assertThat(socketFactory.getDefaultCipherSuites()).isEqualTo(wrapped.getDefaultCipherSuites());
  }

  @Test
  void customSslSocketFactory_getSupportedCipherSuites_delegatesToWrappedFactory() {
    FakeSslSocketFactory wrapped = new FakeSslSocketFactory();
    CustomSslSocketFactory socketFactory = new CustomSslSocketFactory(wrapped, 0);

    assertThat(socketFactory.getSupportedCipherSuites())
        .isEqualTo(wrapped.getSupportedCipherSuites());
  }

  /**
   * An {@link SSLSocketFactory} that hands out unconnected plain sockets, so that socket
   * customization can be asserted without opening a TLS connection.
   */
  private static final class FakeSslSocketFactory extends SSLSocketFactory {

    private static final String[] DEFAULT_CIPHER_SUITES = {"FAKE_DEFAULT_CIPHER_SUITE"};
    private static final String[] SUPPORTED_CIPHER_SUITES = {"FAKE_SUPPORTED_CIPHER_SUITE"};

    @Override
    public String[] getDefaultCipherSuites() {
      return DEFAULT_CIPHER_SUITES.clone();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return SUPPORTED_CIPHER_SUITES.clone();
    }

    @Override
    public Socket createSocket() {
      return new Socket();
    }

    @Override
    public Socket createSocket(Socket s, InputStream consumed, boolean autoClose) {
      return new Socket();
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) {
      return new Socket();
    }

    @Override
    public Socket createSocket(String host, int port) {
      return new Socket();
    }

    @Override
    public Socket createSocket(InetAddress address, int port) {
      return new Socket();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress clientAddress, int clientPort) {
      return new Socket();
    }

    @Override
    public Socket createSocket(
        InetAddress address, int port, InetAddress clientAddress, int clientPort) {
      return new Socket();
    }
  }
}
