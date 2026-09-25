/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GcsObjectRangeTest {

  @Test
  void setPromotionAction_promotedBeforeAttach_runsTheAction() {
    GcsObjectRange range = createRange();
    AtomicInteger promotions = new AtomicInteger();
    range.promote();

    range.setPromotionAction(promotions::incrementAndGet);

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void promote_actionAttached_runsTheActionOnce() {
    GcsObjectRange range = createRange();
    AtomicInteger promotions = new AtomicInteger();
    range.setPromotionAction(promotions::incrementAndGet);

    range.promote();
    range.promote();

    assertThat(promotions.get()).isEqualTo(1);
  }

  @Test
  void isPromoted_afterPromote_returnsTrue() {
    GcsObjectRange range = createRange();

    range.promote();

    assertThat(range.isPromoted()).isTrue();
  }

  private static GcsObjectRange createRange() {
    return GcsObjectRange.builder()
        .setOffset(0)
        .setLength(16)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }
}
