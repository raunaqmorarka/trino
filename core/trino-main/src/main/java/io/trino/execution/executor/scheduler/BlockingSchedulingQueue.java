/*
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
package io.trino.execution.executor.scheduler;

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/// Task updates are posted to a queue and applied in order by whichever thread holds the lock, so no
/// thread waits for the lock to update a task. The holder then hands free slots to runnable tasks in
/// fair order, and checks for updates posted while it held the lock once it releases it.
@ThreadSafe
final class BlockingSchedulingQueue<G, T>
{
    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final SchedulingQueue<G, T> queue = new SchedulingQueue<>();

    private final Queue<Update<G, T>> pending = new ConcurrentLinkedQueue<>();
    private final long expectedWeight;
    private final Dispatcher<T> dispatcher;

    BlockingSchedulingQueue(long expectedWeight, Dispatcher<T> dispatcher)
    {
        this.expectedWeight = expectedWeight;
        this.dispatcher = requireNonNull(dispatcher, "dispatcher is null");
    }

    public void startGroup(G group)
    {
        List<T> started = new ArrayList<>();
        List<T> abandoned = new ArrayList<>();
        lock.lock();
        try {
            applyPending(started, abandoned);
            queue.startGroup(group);
        }
        finally {
            lock.unlock();
        }
        startAndAbandon(started, abandoned);
        drain();
    }

    public Set<T> finishGroup(G group)
    {
        List<T> started = new ArrayList<>();
        List<T> abandoned = new ArrayList<>();
        Set<T> tasks;
        lock.lock();
        try {
            applyPending(started, abandoned);
            tasks = queue.finishGroup(group);
        }
        finally {
            lock.unlock();
        }
        startAndAbandon(started, abandoned);
        drain();
        return tasks;
    }

    public Set<T> getTasks(G group)
    {
        List<T> started = new ArrayList<>();
        List<T> abandoned = new ArrayList<>();
        Set<T> tasks;
        lock.lock();
        try {
            applyPending(started, abandoned);
            if (queue.containsGroup(group)) {
                tasks = queue.getTasks(group);
            }
            else {
                tasks = ImmutableSet.of();
            }
        }
        finally {
            lock.unlock();
        }
        startAndAbandon(started, abandoned);
        drain();
        return tasks;
    }

    public Set<T> finishAll()
    {
        List<T> started = new ArrayList<>();
        List<T> abandoned = new ArrayList<>();
        Set<T> tasks;
        lock.lock();
        try {
            applyPending(started, abandoned);
            tasks = queue.finishAll();
        }
        finally {
            lock.unlock();
        }
        startAndAbandon(started, abandoned);
        drain();
        return tasks;
    }

    /// Make the task runnable. A task whose group is gone is abandoned.
    public void enqueue(G group, T task, long deltaWeight)
    {
        pending.add(new Update<>(UpdateKind.ENQUEUE, group, task, deltaWeight));
        drain();
    }

    /// Mark the running task blocked. A task whose group is gone is abandoned.
    public void block(G group, T task, long deltaWeight)
    {
        pending.add(new Update<>(UpdateKind.BLOCK, group, task, deltaWeight));
        drain();
    }

    /// Remove the task from its group.
    public void finish(G group, T task, long deltaWeight)
    {
        pending.add(new Update<>(UpdateKind.FINISH, group, task, deltaWeight));
        drain();
    }

    /// Hand free slots to runnable tasks.
    public void dispatch()
    {
        pending.add(new Update<>(UpdateKind.DISPATCH, null, null, 0));
        drain();
    }

    /// Apply pending updates and hand out free slots until no update is pending, or until another
    /// thread holds the lock, which then checks for pending updates once it releases it.
    private void drain()
    {
        while (!pending.isEmpty() && lock.tryLock()) {
            List<T> started = new ArrayList<>();
            List<T> abandoned = new ArrayList<>();
            try {
                applyPending(started, abandoned);
            }
            finally {
                lock.unlock();
            }
            startAndAbandon(started, abandoned);
        }
    }

    /// The slot is claimed only when a task is runnable, so it is never held without a task.
    @GuardedBy("lock")
    private void applyPending(List<T> started, List<T> abandoned)
    {
        Update<G, T> update = pending.poll();
        while (update != null) {
            apply(update, abandoned);
            update = pending.poll();
        }
        while (queue.peek() != null && dispatcher.tryReserve()) {
            started.add(queue.dequeue(expectedWeight));
        }
    }

    @GuardedBy("lock")
    private void apply(Update<G, T> update, List<T> abandoned)
    {
        switch (update.kind()) {
            case ENQUEUE -> {
                if (queue.containsGroup(update.group())) {
                    queue.enqueue(update.group(), update.task(), update.deltaWeight());
                }
                else {
                    abandoned.add(update.task());
                }
            }
            case BLOCK -> {
                if (queue.containsGroup(update.group())) {
                    queue.block(update.group(), update.task(), update.deltaWeight());
                }
                else {
                    abandoned.add(update.task());
                }
            }
            case FINISH -> {
                if (queue.containsGroup(update.group())) {
                    queue.finish(update.group(), update.task(), update.deltaWeight());
                }
            }
            case DISPATCH -> {}
        }
    }

    private void startAndAbandon(List<T> started, List<T> abandoned)
    {
        for (T task : abandoned) {
            dispatcher.abandon(task);
        }
        for (T task : started) {
            dispatcher.start(task);
        }
    }

    @Override
    public String toString()
    {
        lock.lock();
        try {
            return queue.toString();
        }
        finally {
            lock.unlock();
        }
    }

    private enum UpdateKind
    {
        ENQUEUE,
        BLOCK,
        FINISH,
        DISPATCH,
    }

    private record Update<G, T>(UpdateKind kind, G group, T task, long deltaWeight) {}

    /// Hands free slots to runnable tasks.
    interface Dispatcher<T>
    {
        /// Claim a free slot. Runs under the queue lock, and only when a task is runnable.
        boolean tryReserve();

        /// Start a task that holds a claimed slot. Runs after the queue lock is released.
        void start(T task);

        /// Release a task whose group is gone. Runs after the queue lock is released.
        void abandon(T task);
    }
}
