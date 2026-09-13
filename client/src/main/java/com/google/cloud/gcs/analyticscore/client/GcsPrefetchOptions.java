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

import com.google.auto.value.AutoValue;
import java.util.Locale;
import java.util.Map;

/** Configuration options for predictive prefetching. */
@AutoValue
public abstract class GcsPrefetchOptions {

  /** Strategy used to decide which bytes are speculatively fetched ahead of the engine. */
  public enum PrefetchMode {
    /**
     * Learns which columns a query touches for a given Parquet schema and prefetches those columns
     * for the current and the next row group.
     */
    PREDICTIVE_ROW_GROUP,

    /** Turns predictive prefetching off. */
    DISABLED
  }

  static final String PREFETCH_MODE_KEY = "analytics-core.prefetch.mode";

  private static final PrefetchMode DEFAULT_PREFETCH_MODE = PrefetchMode.DISABLED;

  /** Returns the prefetching strategy to apply. Defaults to {@code DISABLED}. */
  public abstract PrefetchMode getPrefetchMode();

  /**
   * Returns whether predictive prefetching is enabled, that is whether the prefetch mode is
   * anything other than {@code DISABLED}.
   */
  public final boolean isEnabled() {
    return getPrefetchMode() != PrefetchMode.DISABLED;
  }

  /**
   * Returns a builder for {@link GcsPrefetchOptions} with the same property values as this
   * instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsPrefetchOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsPrefetchOptions.Builder().setPrefetchMode(DEFAULT_PREFETCH_MODE);
  }

  /**
   * Creates a {@link GcsPrefetchOptions} instance from a map of configuration options.
   *
   * <p>Keys absent from the map keep their default value. The prefetch mode is matched
   * case-insensitively and hyphens are treated as underscores, so {@code predictive-row-group}
   * resolves to {@code PREDICTIVE_ROW_GROUP}.
   *
   * @param analyticsCoreOptions the configuration options, keyed without the {@code prefix}
   * @param prefix the prefix prepended to every recognised key
   * @throws IllegalArgumentException if the prefetch mode value does not name a {@link
   *     PrefetchMode}
   */
  public static GcsPrefetchOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsPrefetchOptions.Builder optionsBuilder = builder();
    String prefetchModeFullKey = prefix + PREFETCH_MODE_KEY;
    if (analyticsCoreOptions.containsKey(prefetchModeFullKey)) {
      String value = analyticsCoreOptions.get(prefetchModeFullKey);
      try {
        optionsBuilder.setPrefetchMode(
            PrefetchMode.valueOf(value.replace('-', '_').toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid value '%s' for key '%s'", value, prefetchModeFullKey), e);
      }
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsPrefetchOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets the prefetching strategy to apply. Defaults to {@code DISABLED}. */
    public abstract Builder setPrefetchMode(PrefetchMode prefetchMode);

    public abstract GcsPrefetchOptions build();
  }
}
