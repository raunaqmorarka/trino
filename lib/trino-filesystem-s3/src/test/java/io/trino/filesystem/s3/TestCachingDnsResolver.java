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
package io.trino.filesystem.s3;

import io.airlift.testing.TestingTicker;
import org.apache.hc.client5.http.DnsResolver;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.filesystem.s3.CachingDnsResolver.EXPIRATION;
import static io.trino.filesystem.s3.CachingDnsResolver.REFRESH_INTERVAL;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestCachingDnsResolver
{
    private static final String HOST = "bucket.s3.us-east-1.amazonaws.com";
    private static final InetAddress FIRST_ADDRESS = address(1);
    private static final InetAddress SECOND_ADDRESS = address(2);

    private final TestingDnsResolver delegate = new TestingDnsResolver();
    private final QueuedExecutor refreshExecutor = new QueuedExecutor();
    private final TestingTicker ticker = new TestingTicker();
    private final CachingDnsResolver resolver = new CachingDnsResolver(delegate, refreshExecutor, ticker);

    @Test
    void testFirstLookupResolvesOnCallingThread()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testCachedAddressesWithinRefreshInterval()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(REFRESH_INTERVAL.minusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testStaleAddressesReturnedWhileRefreshing()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(REFRESH_INTERVAL.plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(1);
        assertThat(refreshExecutor.pendingTasks()).isEqualTo(1);

        refreshExecutor.runAll();
        assertThat(delegate.lookups()).isEqualTo(2);
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testFailedRefreshKeepsAddresses()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.removeAddress(HOST);
        advance(REFRESH_INTERVAL.plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        refreshExecutor.runAll();
        assertThat(resolver.resolve(HOST)).containsExactly(FIRST_ADDRESS);
        assertThat(refreshExecutor.pendingTasks()).isEqualTo(1);

        delegate.setAddress(HOST, SECOND_ADDRESS);
        refreshExecutor.runAll();
        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
    }

    @Test
    void testExpiredAddressesResolveOnCallingThread()
            throws UnknownHostException
    {
        delegate.setAddress(HOST, FIRST_ADDRESS);
        resolver.resolve(HOST);
        delegate.setAddress(HOST, SECOND_ADDRESS);
        advance(EXPIRATION.plusSeconds(1));

        assertThat(resolver.resolve(HOST)).containsExactly(SECOND_ADDRESS);
        assertThat(delegate.lookups()).isEqualTo(2);
        assertThat(refreshExecutor.pendingTasks()).isZero();
    }

    @Test
    void testUnknownHost()
    {
        assertThatThrownBy(() -> resolver.resolve(HOST))
                .isInstanceOf(UnknownHostException.class)
                .hasMessage(HOST);
    }

    private void advance(Duration duration)
    {
        ticker.increment(duration.toNanos(), NANOSECONDS);
    }

    private static InetAddress address(int lastOctet)
    {
        try {
            return InetAddress.getByAddress(HOST, new byte[] {10, 0, 0, (byte) lastOctet});
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestingDnsResolver
            implements DnsResolver
    {
        private final Map<String, InetAddress> addresses = new ConcurrentHashMap<>();
        private final AtomicInteger lookups = new AtomicInteger();

        public void setAddress(String host, InetAddress address)
        {
            addresses.put(host, address);
        }

        public void removeAddress(String host)
        {
            addresses.remove(host);
        }

        public int lookups()
        {
            return lookups.get();
        }

        @Override
        public InetAddress[] resolve(String host)
                throws UnknownHostException
        {
            lookups.incrementAndGet();
            InetAddress address = addresses.get(host);
            if (address == null) {
                throw new UnknownHostException(host);
            }
            return new InetAddress[] {address};
        }

        @Override
        public String resolveCanonicalHostname(String host)
        {
            return host;
        }
    }

    private static class QueuedExecutor
            implements Executor
    {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task)
        {
            tasks.add(task);
        }

        public int pendingTasks()
        {
            return tasks.size();
        }

        public void runAll()
        {
            while (!tasks.isEmpty()) {
                tasks.remove().run();
            }
        }
    }
}
