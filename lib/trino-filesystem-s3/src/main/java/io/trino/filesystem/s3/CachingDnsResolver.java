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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.EvictableCacheBuilder;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;

/**
 * Returns cached host addresses and refreshes them on a background executor, so a slow DNS lookup
 * does not block the thread opening a connection. A host unused for longer than the expiration is
 * resolved on the calling thread.
 */
final class CachingDnsResolver
        implements DnsResolver
{
    // The JVM address cache sits under the delegate, so addresses can lag by up to twice this interval
    @VisibleForTesting
    static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);
    @VisibleForTesting
    static final Duration EXPIRATION = Duration.ofMinutes(10);
    private static final long MAX_HOSTS = 1000;

    private final DnsResolver delegate;
    private final LoadingCache<String, List<InetAddress>> addresses;

    public CachingDnsResolver(Executor refreshExecutor)
    {
        this(SystemDefaultDnsResolver.INSTANCE, refreshExecutor, Ticker.systemTicker());
    }

    @VisibleForTesting
    CachingDnsResolver(DnsResolver delegate, Executor refreshExecutor, Ticker ticker)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.addresses = EvictableCacheBuilder.newBuilder()
                .ticker(ticker)
                .refreshAfterWrite(REFRESH_INTERVAL)
                .expireAfterWrite(EXPIRATION)
                .maximumSize(MAX_HOSTS)
                .build(asyncReloading(new CacheLoader<>()
                {
                    @Override
                    public List<InetAddress> load(String host)
                            throws UnknownHostException
                    {
                        return ImmutableList.copyOf(delegate.resolve(host));
                    }
                }, refreshExecutor));
    }

    @Override
    public InetAddress[] resolve(String host)
            throws UnknownHostException
    {
        try {
            return addresses.get(host).toArray(InetAddress[]::new);
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), UnknownHostException.class);
            throw new UncheckedExecutionException(e.getCause());
        }
    }

    @Override
    public String resolveCanonicalHostname(String host)
            throws UnknownHostException
    {
        return delegate.resolveCanonicalHostname(host);
    }
}
