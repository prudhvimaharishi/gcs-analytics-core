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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
  void submitLowPriority_capReached_leavesThreadsForForegroundTasks() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch foregroundDone = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    var unusedSecond = executor.submitLowPriority(() -> {});

    executor.execute(foregroundDone::countDown);

    assertThat(foregroundDone.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void submitLowPriority_capReached_defersTaskUntilASlotFrees() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch secondLowStarted = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    var unusedSecond = executor.submitLowPriority(secondLowStarted::countDown);

    assertThat(secondLowStarted.await(100, TimeUnit.MILLISECONDS)).isFalse();
    lowBlocker.countDown();
  }

  @Test
  void submitLowPriority_runningTaskFinishes_runsDeferredTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch secondLowStarted = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    var unusedSecond = executor.submitLowPriority(secondLowStarted::countDown);

    lowBlocker.countDown();

    assertThat(secondLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void promote_deferredTask_bypassesLowPriorityCapAndRunsOnReservedThread() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch promotedFinished = new CountDownLatch(1);
    var unusedFirst =
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
  void promote_dispatchedTaskInPoolQueue_runsAheadOfEarlierLowPriorityTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 2);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(2);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              executionOrder.add("low-1");
              done.countDown();
            });
    Runnable promoteSecond =
        executor.submitLowPriority(
            () -> {
              executionOrder.add("low-2-promoted");
              done.countDown();
            });

    promoteSecond.run();
    blocker.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("low-2-promoted", "low-1").inOrder();
  }

  @Test
  void cancelFuture_deferredTask_removesTaskFromDeferredQueue() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch cancelledDeferredRan = new CountDownLatch(1);
    CountDownLatch thirdDeferredRan = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange deferredRange =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    executor.submitLowPriority(cancelledDeferredRan::countDown, ImmutableList.of(deferredRange));
    var unusedThird = executor.submitLowPriority(thirdDeferredRan::countDown);

    deferredRange.getByteBufferFuture().cancel(false);
    lowBlocker.countDown();

    assertThat(thirdDeferredRan.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cancelledDeferredRan.await(50, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void cancelFuture_dispatchedTaskInPoolQueue_releasesSlotForDeferredTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch cancelledQueuedRan = new CountDownLatch(1);
    CountDownLatch deferredRan = new CountDownLatch(1);
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange queuedRange =
        GcsObjectRange.builder()
            .setOffset(0)
            .setLength(10)
            .setByteBufferFuture(new CompletableFuture<>())
            .build();
    executor.submitLowPriority(cancelledQueuedRan::countDown, ImmutableList.of(queuedRange));
    var unusedDeferred = executor.submitLowPriority(deferredRan::countDown);

    queuedRange.getByteBufferFuture().cancel(false);
    blocker.countDown();

    assertThat(deferredRan.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cancelledQueuedRan.await(50, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void shutdown_withDeferredTask_dropsTheDeferredTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch deferredRan = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    var unusedDeferred = executor.submitLowPriority(deferredRan::countDown);

    executor.shutdown();
    lowBlocker.countDown();

    assertThat(deferredRan.await(100, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void shutdown_withDeferredTask_cancelsItsRange() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    var unusedFirst =
        executor.submitLowPriority(
            () -> {
              firstLowStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange deferredRange = createRange();
    executor.submitLowPriority(() -> {}, ImmutableList.of(deferredRange));

    executor.shutdown();
    lowBlocker.countDown();

    assertThat(deferredRange.getByteBufferFuture().isCancelled()).isTrue();
  }

  @Test
  void shutdown_withLowPriorityTaskInPoolQueue_cancelsItsRange() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange queuedRange = createRange();
    executor.submitLowPriority(() -> {}, ImmutableList.of(queuedRange));

    executor.shutdown();
    blocker.countDown();

    assertThat(queuedRange.getByteBufferFuture().isCancelled()).isTrue();
  }

  @Test
  void shutdown_withHighPriorityTaskInPoolQueue_runsTheTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch highRan = new CountDownLatch(1);
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    executor.execute(highRan::countDown);

    executor.shutdown();
    blocker.countDown();

    assertThat(highRan.await(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void shutdown_withRunningLowPriorityTask_letsTheTaskFinish() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch lowStarted = new CountDownLatch(1);
    CountDownLatch lowFinished = new CountDownLatch(1);
    executor.submitLowPriority(
        () -> {
          lowStarted.countDown();
          awaitQuietly(lowBlocker);
          lowFinished.countDown();
        },
        ImmutableList.of(createRange()));
    assertThat(lowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    executor.shutdown();
    lowBlocker.countDown();

    assertThat(lowFinished.await(5, TimeUnit.SECONDS)).isTrue();
  }

  private static GcsObjectRange createRange() {
    return GcsObjectRange.builder()
        .setOffset(0)
        .setLength(10)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
