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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.OperatorContext;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static java.util.Objects.requireNonNull;

public class PagePartitionerPool
{
    private final Supplier<PagePartitioner> pagePartitionerSupplier;
    /**
     * Maximum number of free {@link PagePartitioner}s.
     * In normal conditions, in the steady state,
     * the number of free {@link PagePartitioner}s is going to be close to 0.
     * There is a possible case though, where initially big number of concurrent drivers, say 128,
     * drops to a small number e.g., 32 in a steady state. This could cause a lot of memory
     * to be retained by the unused buffers.
     * To defend against that, {@link #maxFree} limits the number of free buffers,
     * thus limiting unused memory.
     */
    private final int maxFree;
    @GuardedBy("this")
    private final Queue<PagePartitioner> free = new ArrayDeque<>();
    @GuardedBy("this")
    private boolean closed;
    // polled and not yet returned to free or closed
    @GuardedBy("this")
    private int outstanding;

    public PagePartitionerPool(int maxFree, Supplier<PagePartitioner> pagePartitionerSupplier)
    {
        this.maxFree = maxFree;
        this.pagePartitionerSupplier = requireNonNull(pagePartitionerSupplier, "pagePartitionerSupplier is null");
    }

    public synchronized PagePartitioner poll()
    {
        checkArgument(!closed, "The pool is already closed");
        PagePartitioner pagePartitioner = free.poll();
        if (pagePartitioner == null) {
            pagePartitioner = pagePartitionerSupplier.get();
        }
        outstanding++;
        return pagePartitioner;
    }

    /**
     * Returns the {@link PagePartitioner} to the pool. When the pool keeps no more free partitioners, or is closed while
     * other partitioners are outstanding, its buffered rows move into a free partitioner. The last partitioner returned
     * after {@link #close()} flushes all free partitioners. It is closed if preparing it for release fails.
     */
    public Metrics release(PagePartitioner pagePartitioner, OperatorContext operatorContext)
    {
        Metrics metrics;
        try {
            metrics = pagePartitioner.prepareForRelease(operatorContext);
        }
        catch (Throwable e) {
            closeAllSuppress(e, pagePartitioner);
            closeAllSuppress(e, takeFreeAfterFailedRelease());
            throw e;
        }

        PagePartitioner target;
        synchronized (this) {
            if (!closed && free.size() < maxFree) {
                free.add(pagePartitioner);
                outstanding--;
                return metrics;
            }
            target = free.poll();
            if (target == null) {
                if (closed && outstanding > 1) {
                    free.add(pagePartitioner);
                    outstanding--;
                    return metrics;
                }
                outstanding--;
            }
        }
        if (target == null) {
            // pagePartitioner.close can take a long time (flush->serialization), we want to keep it out of the synchronized block
            pagePartitioner.close();
            return metrics;
        }

        try {
            target.mergeFrom(pagePartitioner);
        }
        catch (Throwable e) {
            closeAllSuppress(e, target, pagePartitioner);
            closeAllSuppress(e, takeFreeAfterFailedRelease());
            throw e;
        }
        pagePartitioner.close();
        boolean retainTarget;
        Collection<PagePartitioner> toClose;
        synchronized (this) {
            // other releases may have filled the free partitioners during the merge
            retainTarget = closed || free.size() < maxFree;
            if (retainTarget) {
                free.add(target);
            }
            outstanding--;
            toClose = takeFreeIfDone();
        }
        if (!retainTarget) {
            target.close();
        }
        closeSafely(toClose);
        return metrics;
    }

    private synchronized AutoCloseable[] takeFreeAfterFailedRelease()
    {
        outstanding--;
        return takeFreeIfDone().toArray(new AutoCloseable[0]);
    }

    public void close()
    {
        // pagePartitioner.close can take a long time (flush->serialization), we want to keep it out of the synchronized block
        Collection<PagePartitioner> toClose = markClosed();
        closeSafely(toClose);
    }

    private static void closeSafely(Collection<PagePartitioner> toClose)
    {
        try (Closer closer = Closer.create()) {
            toClose.forEach(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized Collection<PagePartitioner> markClosed()
    {
        closed = true;
        return takeFreeIfDone();
    }

    @GuardedBy("this")
    private List<PagePartitioner> takeFreeIfDone()
    {
        if (!closed || outstanding > 0) {
            return ImmutableList.of();
        }
        List<PagePartitioner> toClose = ImmutableList.copyOf(free);
        free.clear();
        return toClose;
    }
}
