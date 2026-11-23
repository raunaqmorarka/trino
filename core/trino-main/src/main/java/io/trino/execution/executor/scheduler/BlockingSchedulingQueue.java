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

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
final class BlockingSchedulingQueue<G, T>
{
    private final Lock lock = new ReentrantLock();

    @GuardedBy("lock")
    private final SchedulingQueue<G, T> queue = new SchedulingQueue<>();

    public void startGroup(G group)
    {
        lock.lock();
        try {
            queue.startGroup(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishGroup(G group)
    {
        lock.lock();
        try {
            return queue.finishGroup(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> getTasks(G group)
    {
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return ImmutableSet.of();
            }

            return queue.getTasks(group);
        }
        finally {
            lock.unlock();
        }
    }

    public Set<T> finishAll()
    {
        lock.lock();
        try {
            return queue.finishAll();
        }
        finally {
            lock.unlock();
        }
    }

    /// Enqueue the task, then hand a free slot to the next runnable task in the same lock hold.
    public boolean enqueue(G group, T task, long deltaWeight, long expectedWeight, Dispatcher<T> dispatcher)
    {
        T next;
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.enqueue(group, task, deltaWeight);
            next = dequeueLocked(expectedWeight, dispatcher);
        }
        finally {
            lock.unlock();
        }
        if (next != null) {
            dispatcher.start(next);
        }
        return true;
    }

    /// Block the task, then hand a free slot to the next runnable task in the same lock hold.
    public boolean block(G group, T task, long deltaWeight, long expectedWeight, Dispatcher<T> dispatcher)
    {
        T next;
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.block(group, task, deltaWeight);
            next = dequeueLocked(expectedWeight, dispatcher);
        }
        finally {
            lock.unlock();
        }
        if (next != null) {
            dispatcher.start(next);
        }
        return true;
    }

    /// Dequeue the next runnable task if `dispatcher` grants it a slot, or return `null` if no task is
    /// runnable or no slot is free.
    public T dequeue(long expectedWeight, Dispatcher<T> dispatcher)
    {
        lock.lock();
        try {
            return dequeueLocked(expectedWeight, dispatcher);
        }
        finally {
            lock.unlock();
        }
    }

    /// Finish the task, then hand a free slot to the next runnable task in the same lock hold.
    public boolean finish(G group, T task, long deltaWeight, long expectedWeight, Dispatcher<T> dispatcher)
    {
        T next;
        lock.lock();
        try {
            if (!queue.containsGroup(group)) {
                return false;
            }

            queue.finish(group, task, deltaWeight);
            next = dequeueLocked(expectedWeight, dispatcher);
        }
        finally {
            lock.unlock();
        }
        if (next != null) {
            dispatcher.start(next);
        }
        return true;
    }

    /// The slot is claimed only when a task is runnable, so it is never held without a task.
    @GuardedBy("lock")
    private T dequeueLocked(long expectedWeight, Dispatcher<T> dispatcher)
    {
        if (queue.peek() == null || !dispatcher.tryReserve()) {
            return null;
        }
        return queue.dequeue(expectedWeight);
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

    /// Hands free slots to runnable tasks.
    interface Dispatcher<T>
    {
        /// Claim a free slot. Runs under the queue lock, and only when a task is runnable.
        boolean tryReserve();

        /// Start a task that holds a claimed slot. Runs after the queue lock is released.
        void start(T task);
    }
}
