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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
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
    submitLowPriority(
        executor,
        () -> {
          executionOrder.add("low-1");
          done.countDown();
        });
    submitLowPriority(
        executor,
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
  void execute_multipleForegroundTasksQueued_runsInSubmissionOrder() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
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
    executor.execute(
        () -> {
          executionOrder.add("high-1");
          done.countDown();
        });
    executor.execute(
        () -> {
          executionOrder.add("high-2");
          done.countDown();
        });

    blocker.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("high-1", "high-2").inOrder();
  }

  @Test
  void submitLowPriority_capReached_leavesThreadsForForegroundTasks() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch foregroundDone = new CountDownLatch(1);
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(executor, () -> {});

    executor.execute(foregroundDone::countDown);

    assertThat(foregroundDone.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void submitLowPriority_capReached_doesNotDeferQueuedForegroundTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch foregroundBlocker = new CountDownLatch(1);
    CountDownLatch blockersStarted = new CountDownLatch(2);
    CountDownLatch nextTaskStarted = new CountDownLatch(1);
    AtomicBoolean queuedForegroundRan = new AtomicBoolean();
    submitLowPriority(
        executor,
        () -> {
          blockersStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    executor.execute(
        () -> {
          blockersStarted.countDown();
          awaitQuietly(foregroundBlocker);
        });
    assertThat(blockersStarted.await(5, TimeUnit.SECONDS)).isTrue();
    executor.execute(
        () -> {
          queuedForegroundRan.set(true);
          nextTaskStarted.countDown();
        });

    submitLowPriority(
        executor,
        () -> {
          nextTaskStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    foregroundBlocker.countDown();

    assertThat(nextTaskStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(queuedForegroundRan.get()).isTrue();
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
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    submitLowPriority(executor, secondLowStarted::countDown);

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
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(executor, secondLowStarted::countDown);

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
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange secondRange = submitLowPriority(executor, promotedFinished::countDown);

    secondRange.promote();

    assertThat(promotedFinished.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void promote_rangeSharedByTwoDeferredTasks_runsBothTasks() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 3, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch sharedRangeTasksFinished = new CountDownLatch(2);
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange sharedRange = createRange();
    executor.submitLowPriority(sharedRangeTasksFinished::countDown, ImmutableList.of(sharedRange));
    executor.submitLowPriority(sharedRangeTasksFinished::countDown, ImmutableList.of(sharedRange));

    sharedRange.promote();

    assertThat(sharedRangeTasksFinished.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void promote_dispatchedTaskInPoolQueue_runsAheadOfEarlierLowPriorityTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 2);
    CountDownLatch firstBlocker = new CountDownLatch(1);
    CountDownLatch secondBlocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(firstBlocker);
        });
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(secondBlocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(
        executor,
        () -> {
          executionOrder.add("low-1");
          done.countDown();
        });
    GcsObjectRange secondRange =
        submitLowPriority(
            executor,
            () -> {
              executionOrder.add("low-2-promoted");
              done.countDown();
            });

    secondRange.promote();
    firstBlocker.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("low-2-promoted", "low-1").inOrder();
    secondBlocker.countDown();
  }

  @Test
  void promote_runningTask_releasesSlotForDeferredTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch runningStarted = new CountDownLatch(1);
    CountDownLatch deferredStarted = new CountDownLatch(1);
    GcsObjectRange runningRange =
        submitLowPriority(
            executor,
            () -> {
              runningStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(runningStarted.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(executor, deferredStarted::countDown);

    runningRange.promote();

    assertThat(deferredStarted.await(5, TimeUnit.SECONDS)).isTrue();
    lowBlocker.countDown();
  }

  @Test
  void promote_cancelledDeferredTask_doesNotRunTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch laterTaskRan = new CountDownLatch(1);
    AtomicBoolean cancelledTaskRan = new AtomicBoolean();
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange cancelledRange = submitLowPriority(executor, () -> cancelledTaskRan.set(true));
    submitLowPriority(executor, laterTaskRan::countDown);
    cancelledRange.getByteBufferFuture().cancel(false);

    cancelledRange.promote();
    lowBlocker.countDown();

    assertThat(laterTaskRan.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cancelledTaskRan.get()).isFalse();
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
    submitLowPriority(
        executor,
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
    submitLowPriority(executor, thirdDeferredRan::countDown);

    deferredRange.getByteBufferFuture().cancel(false);
    lowBlocker.countDown();

    assertThat(thirdDeferredRan.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cancelledDeferredRan.await(50, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void cancelFuture_taskWithAnotherPendingRange_stillRunsTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch deferredRan = new CountDownLatch(1);
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    GcsObjectRange cancelledRange = createRange();
    executor.submitLowPriority(
        deferredRan::countDown, ImmutableList.of(cancelledRange, createRange()));

    cancelledRange.getByteBufferFuture().cancel(false);
    lowBlocker.countDown();

    assertThat(deferredRan.await(5, TimeUnit.SECONDS)).isTrue();
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
    submitLowPriority(executor, deferredRan::countDown);

    queuedRange.getByteBufferFuture().cancel(false);
    blocker.countDown();

    assertThat(deferredRan.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(cancelledQueuedRan.await(50, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void cancelFuture_runningTask_keepsDeferredTaskWaiting() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch runningStarted = new CountDownLatch(1);
    CountDownLatch foregroundDone = new CountDownLatch(1);
    AtomicBoolean deferredStarted = new AtomicBoolean();
    GcsObjectRange runningRange =
        submitLowPriority(
            executor,
            () -> {
              runningStarted.countDown();
              awaitQuietly(lowBlocker);
            });
    assertThat(runningStarted.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(executor, () -> deferredStarted.set(true));

    runningRange.getByteBufferFuture().cancel(false);
    executor.execute(foregroundDone::countDown);

    assertThat(foregroundDone.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(deferredStarted.get()).isFalse();
    lowBlocker.countDown();
  }

  @Test
  void shutdown_withDeferredTask_dropsTheDeferredTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch deferredRan = new CountDownLatch(1);
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    submitLowPriority(executor, deferredRan::countDown);

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
    submitLowPriority(
        executor,
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

  @Test
  void submitLowPriority_deferredTasksWithDifferentSizes_runsSmallestRangeFirst() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 1);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch firstLowStarted = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(3);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    submitLowPriority(
        executor,
        () -> {
          firstLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(firstLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    executor.submitLowPriority(
        () -> {
          executionOrder.add("8MB-data");
          done.countDown();
        },
        ImmutableList.of(createRangeWithLength(8 * 1024 * 1024)));
    executor.submitLowPriority(
        () -> {
          executionOrder.add("4KB-dict-1");
          done.countDown();
        },
        ImmutableList.of(createRangeWithLength(4 * 1024)));
    executor.submitLowPriority(
        () -> {
          executionOrder.add("4KB-dict-2");
          done.countDown();
        },
        ImmutableList.of(createRangeWithLength(4 * 1024)));

    lowBlocker.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("4KB-dict-1", "4KB-dict-2", "8MB-data").inOrder();
  }

  @Test
  void submitLowPriority_dispatchedTasksWithDifferentSizes_runsSmallestRangeFirst()
      throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 2, /* maxLowPriorityConcurrency= */ 2);
    CountDownLatch blocker = new CountDownLatch(1);
    CountDownLatch otherWorkerBlocker = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch done = new CountDownLatch(2);
    List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(blocker);
        });
    executor.execute(
        () -> {
          started.countDown();
          awaitQuietly(otherWorkerBlocker);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    executor.submitLowPriority(
        () -> {
          executionOrder.add("large");
          done.countDown();
        },
        ImmutableList.of(createRangeWithLength(1024)));
    executor.submitLowPriority(
        () -> {
          executionOrder.add("small");
          done.countDown();
        },
        ImmutableList.of(createRangeWithLength(64)));

    blocker.countDown();

    assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executionOrder).containsExactly("small", "large").inOrder();
    otherWorkerBlocker.countDown();
  }

  @Test
  void submitLowPriority_capReached_preemptsLargestQueuedLowPriorityTask() throws Exception {
    executor =
        new PrioritizedReadExecutorService(
            /* threadCount= */ 3, /* maxLowPriorityConcurrency= */ 3);
    CountDownLatch foregroundBlocker = new CountDownLatch(1);
    CountDownLatch foregroundStarted = new CountDownLatch(3);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch threeLowStarted = new CountDownLatch(3);
    List<Integer> startedLengths = Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < 3; i++) {
      executor.execute(
          () -> {
            foregroundStarted.countDown();
            awaitQuietly(foregroundBlocker);
          });
    }
    assertThat(foregroundStarted.await(5, TimeUnit.SECONDS)).isTrue();
    IntConsumer submitLowPriorityOfLength =
        length ->
            executor.submitLowPriority(
                () -> {
                  startedLengths.add(length);
                  threeLowStarted.countDown();
                  awaitQuietly(lowBlocker);
                },
                ImmutableList.of(createRangeWithLength(length)));
    submitLowPriorityOfLength.accept(100);
    submitLowPriorityOfLength.accept(300);
    submitLowPriorityOfLength.accept(200);

    submitLowPriorityOfLength.accept(50);
    foregroundBlocker.countDown();

    assertThat(threeLowStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(startedLengths).containsExactly(50, 100, 200);
    lowBlocker.countDown();
  }

  @Test
  void defaultConstructor_capsLowPriorityTasksAtHalfOfThreadCount() throws Exception {
    executor = new PrioritizedReadExecutorService(/* threadCount= */ 4);
    CountDownLatch lowBlocker = new CountDownLatch(1);
    CountDownLatch twoLowStarted = new CountDownLatch(2);
    CountDownLatch thirdLowStarted = new CountDownLatch(1);
    submitLowPriority(
        executor,
        () -> {
          twoLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    submitLowPriority(
        executor,
        () -> {
          twoLowStarted.countDown();
          awaitQuietly(lowBlocker);
        });
    assertThat(twoLowStarted.await(5, TimeUnit.SECONDS)).isTrue();

    submitLowPriority(executor, thirdLowStarted::countDown);

    assertThat(thirdLowStarted.await(100, TimeUnit.MILLISECONDS)).isFalse();
    lowBlocker.countDown();
  }

  @Test
  void constructor_nonPositiveMaxLowPriorityConcurrency_throwsIllegalArgumentException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new PrioritizedReadExecutorService(
                /* threadCount= */ 1, /* maxLowPriorityConcurrency= */ 0));
  }

  /** Submits {@code command} at {@code LOW} priority for a new range and returns that range. */
  private static GcsObjectRange submitLowPriority(
      PrioritizedReadExecutorService executor, Runnable command) {
    GcsObjectRange range = createRange();
    executor.submitLowPriority(command, ImmutableList.of(range));
    return range;
  }

  private static GcsObjectRange createRange() {
    return createRangeWithLength(10);
  }

  private static GcsObjectRange createRangeWithLength(int length) {
    return GcsObjectRange.builder()
        .setOffset(0)
        .setLength(length)
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
