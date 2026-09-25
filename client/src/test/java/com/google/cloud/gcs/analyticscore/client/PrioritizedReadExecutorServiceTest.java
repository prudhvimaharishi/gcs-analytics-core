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

import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PrioritizedReadExecutorServiceTest {

  private PrioritizedReadExecutorService executor;

  @AfterEach
  void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Test
  void execute_whenLowPriorityTasksQueued_runsForegroundTasksFirst() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(3);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

    executor.submitLowPriority(
        () -> {
          executionOrder.add("low-1");
          done.countDown();
        });
    executor.submitLowPriority(
        () -> {
          executionOrder.add("low-2");
          done.countDown();
        });
    executor.execute(
        () -> {
          executionOrder.add("high-1");
          done.countDown();
        });

    blocker.countDown();
    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("high-1", "low-1", "low-2").inOrder();
  }

  @Test
  void submitLowPriority_capsActiveLowPriorityTasksAndLeavesThreadsForForeground()
      throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch secondLowStarted = new CountDownLatch(1);
    CountDownLatch highCompleted = new CountDownLatch(1);

    executor.submitLowPriority(
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    executor.submitLowPriority(secondLowStarted::countDown);
    executor.execute(highCompleted::countDown);

    assertThat(highCompleted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(secondLowStarted.getCount()).isEqualTo(1L);

    lowBlocker.countDown();
    assertThat(secondLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void promote_whenTaskDeferred_bypassesLowPriorityCapAndRunsOnReservedThread() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch promotedFinished = new CountDownLatch(1);

    executor.submitLowPriority(
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    Runnable promoteSecond = executor.submitLowPriority(promotedFinished::countDown);
    promoteSecond.run();

    assertThat(promotedFinished.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void cancel_whenTaskDeferredOrRunning_removesDeferredAndInterruptsRunningTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch firstStarted = new CountDownLatch(1);
    CountDownLatch firstInterrupted = new CountDownLatch(1);
    CountDownLatch deferredRan = new CountDownLatch(1);
    GcsObjectRange activeRange =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new java.util.concurrent.CompletableFuture<>())
            .build();
    GcsObjectRange deferredRange =
        GcsObjectRange.builder()
            .setOffset(10)
            .setLength(10)
            .setByteBufferFuture(new java.util.concurrent.CompletableFuture<>())
            .build();

    executor.submitLowPriority(
        () -> {
          firstStarted.countDown();
          try {
            new CountDownLatch(1).await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            firstInterrupted.countDown();
          }
        },
        Collections.singletonList(activeRange),
        () -> true);
    assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

    executor.submitLowPriority(
        deferredRan::countDown, Collections.singletonList(deferredRange), () -> true);
    deferredRange.cancel();
    activeRange.cancel();

    assertThat(firstInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(deferredRan.await(100, TimeUnit.MILLISECONDS)).isFalse();
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
