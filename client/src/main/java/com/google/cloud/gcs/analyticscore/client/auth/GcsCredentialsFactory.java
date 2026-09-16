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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Google Cloud {@link Credentials} based on {@link GcsAuthOptions}.
 *
 * <p>Supports 6 standard GCP authentication types: Application Default Credentials (ADC), Compute
 * Engine, Service Account JSON keyfile, Workload Identity Federation (external account
 * credentials), OAuth2 User Credentials, and Unauthenticated access. Applies custom HTTP proxy,
 * read timeout, scoping to {@code https://www.googleapis.com/auth/cloud-platform}, token server
 * URLs, and static service account impersonation.
 */
public final class GcsCredentialsFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GcsCredentialsFactory.class);

  private static final ImmutableList<String> CLOUD_PLATFORM_SCOPES =
      ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");

  private GcsCredentialsFactory() {}

  /**
   * Creates a {@link Credentials} instance according to the specified {@link GcsAuthOptions}.
   *
   * @param options The authentication options.
   * @return A configured {@link Credentials} instance, or {@link NoCredentials} if unauthenticated.
   * @throws IOException If credential resolution, key file reading, or transport creation fails.
   * @throws IllegalArgumentException If the options are internally inconsistent.
   */
  public static Credentials createCredentials(GcsAuthOptions options) throws IOException {
    checkNotNull(options, "options cannot be null");
    return createCredentials(options, new GcsTransportOptionsProvider(options));
  }

  /**
   * Creates a {@link Credentials} instance according to the specified {@link GcsAuthOptions}, using
   * the provided transport provider for token fetching.
   */
  public static Credentials createCredentials(
      GcsAuthOptions options, GcsTransportOptionsProvider transportProvider) throws IOException {
    checkNotNull(options, "options cannot be null");
    checkNotNull(transportProvider, "transportProvider cannot be null");

    try {
      return createCredentials(options, transportProvider.getAuthTransportFactory());
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @VisibleForTesting
  static Credentials createCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    checkNotNull(options, "options cannot be null");
    Optional<String> targetPrincipal = getImpersonationTargetPrincipal(options);
    if (options.getAuthType() == AuthType.UNAUTHENTICATED) {
      checkArgument(
          !targetPrincipal.isPresent(),
          "impersonationServiceAccount cannot be set for UNAUTHENTICATED auth type");
      return NoCredentials.getInstance();
    }

    GoogleCredentials scopedCredentials =
        createCredentialsForAuthType(options, transportFactory).createScoped(CLOUD_PLATFORM_SCOPES);
    GoogleCredentials credentials =
        options
            .getTokenServerUri()
            .map(tokenServerUri -> applyTokenServerUri(scopedCredentials, tokenServerUri))
            .orElse(scopedCredentials);

    return targetPrincipal
        .map(principal -> impersonate(credentials, principal, transportFactory))
        .orElse(credentials);
  }

  private static Optional<String> getImpersonationTargetPrincipal(GcsAuthOptions options) {
    return options
        .getImpersonationServiceAccount()
        .map(String::trim)
        .filter(targetPrincipal -> !targetPrincipal.isEmpty());
  }

  private static GoogleCredentials createCredentialsForAuthType(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    switch (options.getAuthType()) {
      case APPLICATION_DEFAULT:
        return GoogleCredentials.getApplicationDefault(transportFactory);
      case COMPUTE_ENGINE:
        return ComputeEngineCredentials.newBuilder()
            .setHttpTransportFactory(transportFactory)
            .build();
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        return createServiceAccountCredentials(options, transportFactory);
      case WORKLOAD_IDENTITY_FEDERATION:
        return createExternalAccountCredentials(options, transportFactory);
      case USER_CREDENTIALS:
        return createUserCredentials(options, transportFactory);
      default:
        throw new IllegalArgumentException("Unsupported auth type: " + options.getAuthType());
    }
  }

  /** Required fields are guaranteed to be present by {@link GcsAuthOptions.Builder#build}. */
  private static GoogleCredentials createServiceAccountCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    try (FileInputStream keyFileStream =
        new FileInputStream(options.getServiceAccountJsonKeyfile().get())) {
      return ServiceAccountCredentials.fromStream(keyFileStream, transportFactory);
    }
  }

  /** Required fields are guaranteed to be present by {@link GcsAuthOptions.Builder#build}. */
  private static GoogleCredentials createExternalAccountCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) throws IOException {
    try (FileInputStream configFileStream =
        new FileInputStream(options.getWorkloadIdentityCredentialConfigFile().get())) {
      return ExternalAccountCredentials.fromStream(configFileStream, transportFactory);
    }
  }

  /** Required fields are guaranteed to be present by {@link GcsAuthOptions.Builder#build}. */
  private static GoogleCredentials createUserCredentials(
      GcsAuthOptions options, HttpTransportFactory transportFactory) {
    return UserCredentials.newBuilder()
        .setClientId(options.getClientId().get())
        .setClientSecret(options.getClientSecret().get().value())
        .setRefreshToken(options.getRefreshToken().get().value())
        .setHttpTransportFactory(transportFactory)
        .build();
  }

  /** Returns {@code credentials} unchanged if its type does not support a token server URI. */
  private static GoogleCredentials applyTokenServerUri(
      GoogleCredentials credentials, URI tokenServerUri) {
    if (credentials instanceof ServiceAccountCredentials) {
      return ((ServiceAccountCredentials) credentials)
          .toBuilder().setTokenServerUri(tokenServerUri).build();
    }
    if (credentials instanceof UserCredentials) {
      return ((UserCredentials) credentials).toBuilder().setTokenServerUri(tokenServerUri).build();
    }
    LOG.warn(
        "Ignoring configured token server URI {}; credential type {} does not support it.",
        tokenServerUri,
        credentials.getClass().getSimpleName());
    return credentials;
  }

  private static GoogleCredentials impersonate(
      GoogleCredentials sourceCredentials,
      String targetPrincipal,
      HttpTransportFactory transportFactory) {
    LOG.debug("Configuring static impersonation for target service account: {}", targetPrincipal);
    return ImpersonatedCredentials.newBuilder()
        .setSourceCredentials(sourceCredentials)
        .setTargetPrincipal(targetPrincipal)
        .setScopes(CLOUD_PLATFORM_SCOPES)
        .setHttpTransportFactory(transportFactory)
        .build();
  }
}
