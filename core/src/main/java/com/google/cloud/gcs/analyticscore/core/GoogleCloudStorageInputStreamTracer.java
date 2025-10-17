/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.gcs.analyticscore.core;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/** Creates spans for the public methods in {@link GoogleCloudStorageInputStream}. */
public final class GoogleCloudStorageInputStreamTracer {

  private static final String INSTRUMENTATION_NAME =
      GoogleCloudStorageInputStreamTracer.class.getName();
  private static final Tracer TRACER = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);

  /**
   * Starts a new span and returns the scope.
   *
   * @param name the name of the span
   * @return the scope for the new span
   */
  public static Span startSpan(String name) {
    return TRACER.spanBuilder(name).startSpan();
  }
}
