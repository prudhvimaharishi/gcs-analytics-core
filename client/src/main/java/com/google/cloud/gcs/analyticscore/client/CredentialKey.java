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
package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.NoCredentials;
import com.google.cloud.gcs.analyticscore.client.auth.AuthType;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Immutable identity key representing the authentication principal and parameters for a cached
 * {@link GcsClientImpl}, with all secret material hashed via SHA-256.
 */
@AutoValue
abstract class CredentialKey {

  enum Kind {
    APPLICATION_DEFAULT,
    NO_CREDENTIALS,
    COMPUTE_ENGINE,
    SERVICE_ACCOUNT,
    USER,
    IMPERSONATED,
    VENDED_OAUTH2,
    WORKLOAD_IDENTITY_FEDERATION,
    UNIQUE_INSTANCE
  }

  abstract Kind kind();

  abstract ImmutableList<String> components();

  abstract OptionalLong expirationTimeMillis();

  static CredentialKey create(
      Kind kind, Iterable<String> components, OptionalLong expirationTimeMillis) {
    return new AutoValue_CredentialKey(
        kind, ImmutableList.copyOf(components), expirationTimeMillis);
  }

  /**
   * Builds a {@link CredentialKey} directly from {@link GcsAuthOptions} without constructing a
   * {@link Credentials} instance or reading full keyfile contents on cache hits.
   */
  static CredentialKey fromAuthOptions(GcsAuthOptions authOptions) {
    checkNotNull(authOptions, "authOptions should not be null");
    AuthType effectiveType = authOptions.effectiveAuthType();
    String tokenServerUri = authOptions.getTokenServerUri().map(URI::toString).orElse("");
    String impersonationTarget = authOptions.getImpersonationServiceAccount().orElse("");

    switch (effectiveType) {
      case UNAUTHENTICATED:
        return create(Kind.NO_CREDENTIALS, ImmutableList.of(), OptionalLong.empty());
      case APPLICATION_DEFAULT:
        return create(
            Kind.APPLICATION_DEFAULT,
            ImmutableList.of(
                Boolean.toString(authOptions.allowsUnauthenticatedFallback()),
                tokenServerUri,
                impersonationTarget),
            OptionalLong.empty());
      case COMPUTE_ENGINE:
        return create(
            Kind.COMPUTE_ENGINE,
            ImmutableList.of(tokenServerUri, impersonationTarget),
            OptionalLong.empty());
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        return createFileBackedKey(
            Kind.SERVICE_ACCOUNT,
            authOptions.getServiceAccountJsonKeyfile().get(),
            tokenServerUri,
            impersonationTarget);
      case WORKLOAD_IDENTITY_FEDERATION:
        return createFileBackedKey(
            Kind.WORKLOAD_IDENTITY_FEDERATION,
            authOptions.getWorkloadIdentityCredentialConfigFile().get(),
            tokenServerUri,
            impersonationTarget);
      default:
        // USER_CREDENTIALS (the only remaining AuthType value)
        return create(
            Kind.USER,
            ImmutableList.of(
                authOptions.getClientId().orElse(""),
                sha256Hex(authOptions.getClientSecret().get().value()),
                sha256Hex(authOptions.getRefreshToken().get().value()),
                tokenServerUri,
                impersonationTarget),
            OptionalLong.empty());
    }
  }

  /**
   * Builds a {@link CredentialKey} from a pre-constructed {@link Credentials} instance using strict
   * runtime class equality and failing closed ({@link Kind#UNIQUE_INSTANCE}) for unrecognized
   * classes.
   */
  static CredentialKey fromCredentials(@Nullable Credentials credentials) {
    if (credentials == null) {
      return create(Kind.APPLICATION_DEFAULT, ImmutableList.of(), OptionalLong.empty());
    }
    Class<?> clazz = credentials.getClass();
    if (clazz == NoCredentials.class) {
      return create(Kind.NO_CREDENTIALS, ImmutableList.of(), OptionalLong.empty());
    }
    if (clazz == ComputeEngineCredentials.class) {
      ComputeEngineCredentials c = (ComputeEngineCredentials) credentials;
      ComputeEngineCredentials.Builder b = c.toBuilder();
      return create(
          Kind.COMPUTE_ENGINE,
          ImmutableList.of(
              nullToEmpty(b.getUniverseDomain()),
              nullToEmpty(c.getQuotaProjectId()),
              sortedCollection(c.getScopes()),
              sortedCollection(b.getDefaultScopes())),
          OptionalLong.empty());
    }
    if (clazz == ServiceAccountCredentials.class) {
      ServiceAccountCredentials c = (ServiceAccountCredentials) credentials;
      byte[] encodedKey = c.getPrivateKey().getEncoded();
      ServiceAccountCredentials.Builder b = c.toBuilder();
      return create(
          Kind.SERVICE_ACCOUNT,
          ImmutableList.of(
              nullToEmpty(c.getClientId()),
              nullToEmpty(c.getClientEmail()),
              nullToEmpty(c.getPrivateKeyId()),
              encodedKey != null ? sha256Hex(encodedKey) : "",
              nullToEmpty(c.getServiceAccountUser()),
              sortedCollection(c.getScopes()),
              sortedCollection(c.getDefaultScopes()),
              Objects.toString(c.getTokenServerUri(), ""),
              nullToEmpty(c.getQuotaProjectId()),
              nullToEmpty(b.getUniverseDomain()),
              nullToEmpty(c.getProjectId()),
              Boolean.toString(c.getUseJwtAccessWithScope()),
              Integer.toString(b.getLifetime())),
          OptionalLong.empty());
    }
    if (clazz == UserCredentials.class) {
      return fromUserCredentials((UserCredentials) credentials);
    }
    if (clazz == ImpersonatedCredentials.class) {
      ImpersonatedCredentials c = (ImpersonatedCredentials) credentials;
      CredentialKey sourceKey = fromCredentials(c.getSourceCredentials());
      if (sourceKey.kind() == Kind.UNIQUE_INSTANCE) {
        return uniqueInstance();
      }
      ImpersonatedCredentials.Builder b = c.toBuilder();
      ImmutableList.Builder<String> components = ImmutableList.builder();
      components.add(sourceKey.kind().name());
      components.addAll(sourceKey.components());
      components.add(nullToEmpty(c.getAccount()));
      components.add(sortedCollection(b.getScopes()));
      components.add(Objects.toString(b.getDelegates(), "[]"));
      components.add(Integer.toString(b.getLifetime()));
      components.add(nullToEmpty(c.getQuotaProjectId()));
      components.add(nullToEmpty(b.getUniverseDomain()));
      return create(Kind.IMPERSONATED, components.build(), sourceKey.expirationTimeMillis());
    }
    if (clazz == OAuth2Credentials.class || clazz == OAuth2CredentialsWithRefresh.class) {
      OAuth2Credentials c = (OAuth2Credentials) credentials;
      AccessToken token = c.getAccessToken();
      if (token == null || isNullOrEmpty(token.getTokenValue())) {
        return uniqueInstance();
      }
      OptionalLong expMillis = tokenExpirationMillis(token);
      return create(
          Kind.VENDED_OAUTH2,
          ImmutableList.of(
              clazz.getName(),
              sha256Hex(token.getTokenValue()),
              expMillis.isPresent() ? Long.toString(expMillis.getAsLong()) : ""),
          expMillis);
    }
    return uniqueInstance();
  }

  /**
   * Keys {@link UserCredentials} on the refresh token when present. Without a refresh token the
   * access token is the only field identifying the user, so it becomes part of the key and the key
   * expires with it.
   */
  private static CredentialKey fromUserCredentials(UserCredentials c) {
    UserCredentials.Builder b = c.toBuilder();
    String refreshTokenHash = "";
    String accessTokenHash = "";
    OptionalLong expirationTimeMillis = OptionalLong.empty();
    if (!isNullOrEmpty(c.getRefreshToken())) {
      // The access token is deliberately excluded: refresh() rotates it in place.
      refreshTokenHash = sha256Hex(c.getRefreshToken());
    } else {
      AccessToken token = c.getAccessToken();
      if (token == null || isNullOrEmpty(token.getTokenValue())) {
        return uniqueInstance();
      }
      accessTokenHash = sha256Hex(token.getTokenValue());
      expirationTimeMillis = tokenExpirationMillis(token);
    }
    return create(
        Kind.USER,
        ImmutableList.of(
            nullToEmpty(c.getClientId()),
            sha256Hex(nullToEmpty(c.getClientSecret())),
            refreshTokenHash,
            accessTokenHash,
            Objects.toString(b.getTokenServerUri(), ""),
            nullToEmpty(c.getQuotaProjectId()),
            nullToEmpty(b.getUniverseDomain())),
        expirationTimeMillis);
  }

  private static OptionalLong tokenExpirationMillis(AccessToken token) {
    Date expiresAt = token.getExpirationTime();
    return expiresAt != null
        ? OptionalLong.of(expiresAt.toInstant().toEpochMilli())
        : OptionalLong.empty();
  }

  private static CredentialKey createFileBackedKey(
      Kind kind, String configuredPath, String tokenServerUri, String impersonationTarget) {
    try {
      Path realPath = Paths.get(configuredPath).toRealPath();
      long lastModifiedMillis = Files.getLastModifiedTime(realPath).toMillis();
      long fileSize = Files.size(realPath);
      return create(
          kind,
          ImmutableList.of(
              realPath.toString(),
              Long.toString(lastModifiedMillis),
              Long.toString(fileSize),
              tokenServerUri,
              impersonationTarget),
          OptionalLong.empty());
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read credential file metadata: " + configuredPath, e);
    }
  }

  private static CredentialKey uniqueInstance() {
    return create(
        Kind.UNIQUE_INSTANCE, ImmutableList.of(UUID.randomUUID().toString()), OptionalLong.empty());
  }

  static String sha256Hex(String value) {
    return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
  }

  static String sha256Hex(byte[] bytes) {
    return Hashing.sha256().hashBytes(bytes).toString();
  }

  private static String sortedCollection(@Nullable Collection<String> values) {
    if (values == null || values.isEmpty()) {
      return "[]";
    }
    return values.stream().sorted().collect(Collectors.joining(",", "[", "]"));
  }
}
