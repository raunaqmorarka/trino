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

import io.trino.annotation.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

@NotThreadSafe
final class PriorityQueue<T>
{
    private final Map<T, Entry<T>> entries = new HashMap<>();
    private final TreeSet<Entry<T>> queue = new TreeSet<>((first, second) -> {
        int result = Long.compare(first.priority(), second.priority());
        if (result == 0) {
            result = Long.compare(first.sequence(), second.sequence());
        }
        return result;
    });

    private long sequence;

    public void add(T value, long priority)
    {
        checkArgument(!entries.containsKey(value), "Value already in queue: %s", value);
        insert(value, priority);
    }

    public void addOrReplace(T value, long priority)
    {
        Entry<T> previous = entries.get(value);
        if (previous != null) {
            queue.remove(previous);
        }
        insert(value, priority);
    }

    public T takeOrThrow()
    {
        T result = poll();
        checkState(result != null, "Queue is empty");
        return result;
    }

    public T poll()
    {
        Entry<T> first = queue.pollFirst();
        if (first == null) {
            return null;
        }
        entries.remove(first.value());
        return first.value();
    }

    public void remove(T value)
    {
        Entry<T> entry = entries.remove(value);
        checkArgument(entry != null, "Value not in queue: %s", value);
        queue.remove(entry);
    }

    public void removeIfPresent(T value)
    {
        Entry<T> entry = entries.remove(value);
        if (entry != null) {
            queue.remove(entry);
        }
    }

    public boolean contains(T value)
    {
        return entries.containsKey(value);
    }

    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    public Set<T> values()
    {
        return entries.keySet();
    }

    public long nextPriority()
    {
        checkState(!queue.isEmpty(), "Queue is empty");
        return queue.first().priority();
    }

    public T peek()
    {
        if (queue.isEmpty()) {
            return null;
        }
        return queue.first().value();
    }

    public int size()
    {
        return queue.size();
    }

    @Override
    public String toString()
    {
        return queue.stream()
                .map(Entry::value)
                .collect(toImmutableList())
                .toString();
    }

    private void insert(T value, long priority)
    {
        Entry<T> entry = new Entry<>(value, priority, sequence++);
        entries.put(value, entry);
        queue.add(entry);
    }

    private record Entry<T>(T value, long priority, long sequence) {}
}
