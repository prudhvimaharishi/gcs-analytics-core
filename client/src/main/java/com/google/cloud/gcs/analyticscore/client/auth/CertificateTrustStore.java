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

/** The trust store used to validate TLS certificates presented by the storage endpoint. */
enum CertificateTrustStore {

  /**
   * Google's bundled certificate trust store, which pins trust to the CAs that sign default Google
   * endpoints. This is the correct choice for {@code googleapis.com}.
   */
  GOOGLE_BUNDLED,

  /**
   * The JVM's default trust store. Custom endpoints, such as Trusted Partner Cloud or non-default
   * universe domains, are signed by CAs that are absent from Google's bundle, so they can only be
   * validated against the trust store the JVM was configured with.
   */
  SYSTEM_DEFAULT;
}
