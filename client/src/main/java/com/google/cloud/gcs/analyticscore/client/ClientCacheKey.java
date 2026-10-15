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

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.client.auth.GcsAuthOptions;
import java.time.Duration;
import java.util.Optional;

/**
 * Composite cache key combining a {@link CredentialKey} with transport and client configuration
 * options for {@link GcsClientCache}.
 */
@AutoValue
abstract class ClientCacheKey {

  abstract CredentialKey credentialKey();

  abstract GcsClientOptions clientOptions();

  abstract String transportType();

  abstract int readThreadCount();

  abstract Duration httpConnectTimeout();

  abstract Duration httpReadTimeout();

  abstract Optional<String> proxyAddress();

  abstract Optional<String> proxyUsernameHash();

  abstract Optional<String> proxyPasswordHash();

  @SuppressWarnings("deprecation")
  static ClientCacheKey create(
      CredentialKey credentialKey, GcsFileSystemOptions fileSystemOptions) {
    checkNotNull(credentialKey, "credentialKey should not be null");
    checkNotNull(fileSystemOptions, "fileSystemOptions should not be null");
    GcsAuthOptions authOptions = fileSystemOptions.getGcsAuthOptions();
    return new AutoValue_ClientCacheKey(
        credentialKey,
        fileSystemOptions.getGcsClientOptions(),
        fileSystemOptions.getClientType().name(),
        fileSystemOptions.getReadThreadCount(),
        authOptions.getHttpConnectTimeout(),
        authOptions.getHttpReadTimeout(),
        authOptions.getProxyAddress(),
        authOptions.getProxyUsername().map(secret -> CredentialKey.sha256Hex(secret.value())),
        authOptions.getProxyPassword().map(secret -> CredentialKey.sha256Hex(secret.value())));
  }
}
