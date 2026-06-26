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

/**
 * Identifies a single fixed-size chunk of a GCS object, used as the key for object-chunk caching.
 *
 * <p>The key pins the object's {@code generation} so that overwriting an object (which changes its
 * generation) makes previously cached chunks unreachable instead of stale.
 */
@AutoValue
public abstract class GcsObjectChunkKey {

  /** The bucket/object this chunk belongs to. */
  public abstract GcsItemId getItemId();

  /** The object generation this chunk was read from, or a negative value if unknown. */
  public abstract long getGeneration();

  /** The zero-based chunk index, i.e. {@code offset / chunkSizeBytes}. */
  public abstract long getChunkIndex();

  public static Builder builder() {
    return new AutoValue_GcsObjectChunkKey.Builder();
  }

  /** Builder for {@link GcsObjectChunkKey}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setItemId(GcsItemId itemId);

    public abstract Builder setGeneration(long generation);

    public abstract Builder setChunkIndex(long chunkIndex);

    public abstract GcsObjectChunkKey build();
  }
}
