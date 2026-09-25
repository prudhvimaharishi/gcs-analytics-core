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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Single unified {@link ThreadPoolExecutor} for foreground vectored reads and background
 * speculative prefetches, backed by a {@link PriorityBlockingQueue}.
 *
 * <p>Foreground tasks run at {@code HIGH} priority and may use all threads in the pool. Background
 * speculative tasks run at {@code LOW} priority, are capped at {@code maxLowPriorityConcurrency}
 * active slots so threads remain immediately available for incoming foreground reads, and can be
 * promoted to {@code HIGH} priority in-place when a foreground read demands their bytes.
 */
final class PrioritizedReadExecutorService extends ThreadPoolExecutor {

  private static final int PRIORITY_HIGH = 0;
  private static final int PRIORITY_LOW = 1;
  private static final long IDLE_THREAD_KEEP_ALIVE_SECONDS = 30L;

  private final int maxLowPriorityConcurrency;
  private final AtomicLong sequenceGenerator = new AtomicLong();
  private final Object lock = new Object();
  private final Queue<PrioritizedTask> deferredLowPriorityQueue = new ArrayDeque<>();
  private int activeLowPrioritySlots = 0;

  PrioritizedReadExecutorService(int threadCount) {
    this(threadCount, Math.max(1, (threadCount * 5) / 8));
  }

  PrioritizedReadExecutorService(int threadCount, int maxLowPriorityConcurrency) {
    super(
        threadCount,
        threadCount,
        IDLE_THREAD_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        new PriorityBlockingQueue<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("gcs-filesystem-range-pool-%d")
            .setDaemon(true)
            .build());
    checkArgument(maxLowPriorityConcurrency > 0, "maxLowPriorityConcurrency must be positive");
    this.maxLowPriorityConcurrency = Math.min(threadCount, maxLowPriorityConcurrency);
    allowCoreThreadTimeOut(true);
  }

  @Override
  public void execute(Runnable command) {
    checkNotNull(command, "command cannot be null");
    if (command instanceof PrioritizedTask) {
      super.execute(command);
      return;
    }
    PrioritizedTask task =
        new PrioritizedTask(command, PRIORITY_HIGH, sequenceGenerator.getAndIncrement(), false);
    super.execute(task);
  }

  /**
   * Submits a background prefetch task at {@code LOW} priority and returns a callback that promotes
   * the task to {@code HIGH} priority if a foreground reader requests its range.
   */
  Runnable submitLowPriority(Runnable command) {
    PrioritizedTask task = enqueueLowPriorityTask(command);
    return () -> promote(task);
  }

  /**
   * Submits a background prefetch task at {@code LOW} priority and wires both promotion and
   * deferred-queue cancellation callbacks onto the associated ranges.
   */
  void submitLowPriority(
      Runnable command, List<GcsObjectRange> underlyingRanges, BooleanSupplier allRangesCancelled) {
    PrioritizedTask task = enqueueLowPriorityTask(command);
    Runnable promoter = () -> promote(task);
    Runnable canceller =
        () -> {
          if (allRangesCancelled.getAsBoolean()) {
            cancelDeferredTask(task);
          }
        };
    for (GcsObjectRange childRange : underlyingRanges) {
      childRange.setPromotionAction(promoter);
      childRange.setCancellationAction(canceller);
    }
  }

  private PrioritizedTask enqueueLowPriorityTask(Runnable command) {
    checkNotNull(command, "command cannot be null");
    PrioritizedTask task =
        new PrioritizedTask(command, PRIORITY_LOW, sequenceGenerator.getAndIncrement(), true);
    synchronized (lock) {
      if (activeLowPrioritySlots < maxLowPriorityConcurrency) {
        activeLowPrioritySlots++;
        task.dispatchedToPool = true;
        super.execute(task);
      } else {
        deferredLowPriorityQueue.add(task);
      }
    }
    return task;
  }

  private void promote(PrioritizedTask task) {
    synchronized (lock) {
      if (!task.countsAsLowPriority) {
        return;
      }
      task.countsAsLowPriority = false;
      task.priority = PRIORITY_HIGH;
      if (!task.dispatchedToPool) {
        if (deferredLowPriorityQueue.remove(task)) {
          task.dispatchedToPool = true;
          super.execute(task);
        }
        return;
      }
      if (getQueue().remove(task)) {
        super.execute(task);
      }
      releaseLowPrioritySlotLocked();
    }
  }

  private void cancelDeferredTask(PrioritizedTask task) {
    synchronized (lock) {
      if (!task.dispatchedToPool) {
        deferredLowPriorityQueue.remove(task);
      }
    }
  }

  private void onTaskFinished(PrioritizedTask task) {
    synchronized (lock) {
      if (task.countsAsLowPriority) {
        task.countsAsLowPriority = false;
        releaseLowPrioritySlotLocked();
      }
    }
  }

  private void releaseLowPrioritySlotLocked() {
    PrioritizedTask nextDeferred = deferredLowPriorityQueue.poll();
    if (nextDeferred != null) {
      nextDeferred.dispatchedToPool = true;
      super.execute(nextDeferred);
    } else if (activeLowPrioritySlots > 0) {
      activeLowPrioritySlots--;
    }
  }

  @Override
  public void shutdown() {
    synchronized (lock) {
      deferredLowPriorityQueue.clear();
    }
    super.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    synchronized (lock) {
      deferredLowPriorityQueue.clear();
    }
    return super.shutdownNow();
  }

  private final class PrioritizedTask implements Runnable, Comparable<PrioritizedTask> {
    private final Runnable command;
    private final long sequenceNumber;
    private volatile int priority;
    private boolean countsAsLowPriority;
    private boolean dispatchedToPool;

    private PrioritizedTask(
        Runnable command, int priority, long sequenceNumber, boolean countsAsLowPriority) {
      this.command = command;
      this.priority = priority;
      this.sequenceNumber = sequenceNumber;
      this.countsAsLowPriority = countsAsLowPriority;
    }

    @Override
    public void run() {
      try {
        command.run();
      } finally {
        onTaskFinished(this);
      }
    }

    @Override
    public int compareTo(PrioritizedTask other) {
      int priorityDiff = Integer.compare(this.priority, other.priority);
      return priorityDiff != 0
          ? priorityDiff
          : Long.compare(this.sequenceNumber, other.sequenceNumber);
    }
  }
}
