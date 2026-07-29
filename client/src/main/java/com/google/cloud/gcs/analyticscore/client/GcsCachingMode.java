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

/** Defines the operating mode of the executor-level cache in {@link AnalyticsCacheManager}. */
public enum GcsCachingMode {
  /**
   * Caching is local to the individual client or filesystem instance. No sharing across instances.
   */
  PER_INSTANCE,

  /**
   * Caching is shared executor-wide (JVM-wide), partitioned by the underlying credential identity
   * to prevent cross-principal data leakage.
   */
  SHARED_PER_CREDENTIAL,

  /**
   * Caching is shared executor-wide (JVM-wide) globally across all credentials. Only safe for
   * buckets with Uniform Bucket-Level Access (UBLA) enabled and no object-level ACLs.
   */
  SHARED_GLOBAL
}
