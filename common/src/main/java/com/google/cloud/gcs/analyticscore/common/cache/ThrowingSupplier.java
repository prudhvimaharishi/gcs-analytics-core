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

package com.google.cloud.gcs.analyticscore.common.cache;

/**
 * A supplier that may throw a checked exception, used for lazily loading cache values.
 *
 * @param <V> The type of the supplied value.
 * @param <E> The type of the exception that may be thrown.
 */
@FunctionalInterface
public interface ThrowingSupplier<V, E extends Exception> {

  /** Returns a value, potentially throwing an exception. */
  V get() throws E;
}
