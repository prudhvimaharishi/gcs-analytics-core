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

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/** Configuration options for Google Cloud Storage authentication and network proxy. */
@AutoValue
public abstract class GcsAuthOptions {

  public static final String AUTH_TYPE_KEY = "auth.type";
  public static final String SERVICE_ACCOUNT_JSON_KEYFILE_KEY = "auth.service.account.json.keyfile";
  public static final String WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_KEY =
      "auth.workload.identity.federation.credential.config.file";
  public static final String AUTH_CLIENT_ID_KEY = "auth.client.id";
  public static final String AUTH_CLIENT_SECRET_KEY = "auth.client.secret";
  public static final String AUTH_REFRESH_TOKEN_KEY = "auth.refresh.token";
  public static final String IMPERSONATION_SERVICE_ACCOUNT_KEY =
      "auth.impersonation.service.account";
  public static final String TOKEN_SERVER_URL_KEY = "token.server.url";
  public static final String AUTH_TOKEN_SERVER_URL_KEY = "auth.token.server.url";
  public static final String PROXY_ADDRESS_KEY = "proxy.address";
  public static final String PROXY_USERNAME_KEY = "proxy.username";
  public static final String PROXY_PASSWORD_KEY = "proxy.password";
  public static final String HTTP_READ_TIMEOUT_KEY = "http.read-timeout";
  public static final String HTTP_READ_TIMEOUT_ALT_KEY = "http.read.timeout";

  private static final Duration DEFAULT_HTTP_READ_TIMEOUT = Duration.ofSeconds(5);

  public abstract AuthType getAuthType();

  public abstract Optional<String> getServiceAccountJsonKeyfile();

  public abstract Optional<String> getWorkloadIdentityPoolConfigFile();

  public abstract Optional<String> getClientId();

  public abstract Optional<String> getClientSecret();

  public abstract Optional<String> getRefreshToken();

  public abstract Optional<String> getImpersonationServiceAccount();

  public abstract Optional<URI> getTokenServerUri();

  public abstract Optional<String> getProxyAddress();

  public abstract Optional<String> getProxyUsername();

  public abstract Optional<String> getProxyPassword();

  public abstract Duration getHttpReadTimeout();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsAuthOptions.Builder()
        .setAuthType(AuthType.APPLICATION_DEFAULT)
        .setHttpReadTimeout(DEFAULT_HTTP_READ_TIMEOUT);
  }

  /**
   * Creates a {@link GcsAuthOptions} instance from a configuration properties map.
   *
   * @param options The map containing configuration properties.
   * @param prefix The prefix prepended to configuration property keys. May be {@code null} or
   *     empty, in which case keys are looked up without a prefix. If non-empty and does not end
   *     with a dot, a dot will be automatically appended.
   * @return A configured {@link GcsAuthOptions} instance.
   */
  public static GcsAuthOptions createFromOptions(
      Map<String, String> options, @Nullable String prefix) {
    checkNotNull(options, "options cannot be null");
    String normalizedPrefix =
        prefix == null || prefix.isEmpty() ? "" : (prefix.endsWith(".") ? prefix : prefix + ".");

    Builder builder = builder();
    getTrimmedValue(options, normalizedPrefix, AUTH_TYPE_KEY)
        .ifPresent(value -> builder.setAuthType(AuthType.fromString(value)));
    getTrimmedValue(options, normalizedPrefix, SERVICE_ACCOUNT_JSON_KEYFILE_KEY)
        .ifPresent(builder::setServiceAccountJsonKeyfile);
    getTrimmedValue(
            options, normalizedPrefix, WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_KEY)
        .ifPresent(builder::setWorkloadIdentityPoolConfigFile);
    getTrimmedValue(options, normalizedPrefix, AUTH_CLIENT_ID_KEY).ifPresent(builder::setClientId);
    getTrimmedValue(options, normalizedPrefix, AUTH_CLIENT_SECRET_KEY)
        .ifPresent(builder::setClientSecret);
    getTrimmedValue(options, normalizedPrefix, AUTH_REFRESH_TOKEN_KEY)
        .ifPresent(builder::setRefreshToken);
    getTrimmedValue(options, normalizedPrefix, IMPERSONATION_SERVICE_ACCOUNT_KEY)
        .ifPresent(builder::setImpersonationServiceAccount);
    getTrimmedValue(options, normalizedPrefix, TOKEN_SERVER_URL_KEY)
        .or(() -> getTrimmedValue(options, normalizedPrefix, AUTH_TOKEN_SERVER_URL_KEY))
        .ifPresent(value -> builder.setTokenServerUri(URI.create(value)));
    getTrimmedValue(options, normalizedPrefix, PROXY_ADDRESS_KEY)
        .ifPresent(builder::setProxyAddress);
    getTrimmedValue(options, normalizedPrefix, PROXY_USERNAME_KEY)
        .ifPresent(builder::setProxyUsername);
    getTrimmedValue(options, normalizedPrefix, PROXY_PASSWORD_KEY)
        .ifPresent(builder::setProxyPassword);
    getTrimmedValue(options, normalizedPrefix, HTTP_READ_TIMEOUT_KEY)
        .or(() -> getTrimmedValue(options, normalizedPrefix, HTTP_READ_TIMEOUT_ALT_KEY))
        .ifPresent(value -> builder.setHttpReadTimeout(parseDuration(value)));

    return builder.build();
  }

  /**
   * Looks up {@code normalizedPrefix + key} in the given map.
   *
   * @return the trimmed value, or {@link Optional#empty()} if it is absent or blank.
   */
  private static Optional<String> getTrimmedValue(
      Map<String, String> options, String normalizedPrefix, String key) {
    String value = options.get(normalizedPrefix + key);
    if (value == null || value.trim().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(value.trim());
  }

  private static Duration parseDuration(String str) {
    String lower = str.toLowerCase(Locale.ROOT);
    try {
      if (lower.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2).trim()));
      }
      if (lower.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
      if (lower.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
      return Duration.ofMillis(Long.parseLong(lower));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration string: " + str, e);
    }
  }

  /** Builder for {@link GcsAuthOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setAuthType(AuthType authType);

    public abstract Builder setServiceAccountJsonKeyfile(
        @Nullable String serviceAccountJsonKeyfile);

    public abstract Builder setWorkloadIdentityPoolConfigFile(
        @Nullable String workloadIdentityPoolConfigFile);

    public abstract Builder setClientId(@Nullable String clientId);

    public abstract Builder setClientSecret(@Nullable String clientSecret);

    public abstract Builder setRefreshToken(@Nullable String refreshToken);

    public abstract Builder setImpersonationServiceAccount(
        @Nullable String impersonationServiceAccount);

    public abstract Builder setTokenServerUri(@Nullable URI tokenServerUri);

    public abstract Builder setProxyAddress(@Nullable String proxyAddress);

    public abstract Builder setProxyUsername(@Nullable String proxyUsername);

    public abstract Builder setProxyPassword(@Nullable String proxyPassword);

    public abstract Builder setHttpReadTimeout(Duration httpReadTimeout);

    public abstract GcsAuthOptions build();
  }
}
