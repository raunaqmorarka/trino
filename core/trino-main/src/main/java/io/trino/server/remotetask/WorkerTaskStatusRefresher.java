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
package io.trino.server.remotetask;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStatus;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.server.InternalHeaders.TRINO_NODE_INSTANCE_ID_HEADER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Refreshes the status of all unfinished tasks of a worker with one request per worker and interval.
 * Per-task status requests then only need to return early on task state changes.
 */
public class WorkerTaskStatusRefresher
{
    private static final Logger log = Logger.get(WorkerTaskStatusRefresher.class);

    private final HttpClient httpClient;
    private final JsonCodec<List<TaskStatus>> taskStatusListCodec;
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final Duration refreshInterval;
    private final Map<Worker, WorkerTasks> workers = new ConcurrentHashMap<>();

    public WorkerTaskStatusRefresher(
            HttpClient httpClient,
            JsonCodec<List<TaskStatus>> taskStatusListCodec,
            Executor executor,
            ScheduledExecutorService scheduledExecutor,
            Duration refreshInterval)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskStatusListCodec = requireNonNull(taskStatusListCodec, "taskStatusListCodec is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.refreshInterval = requireNonNull(refreshInterval, "refreshInterval is null");
    }

    public void start()
    {
        long intervalMillis = refreshInterval.toMillis();
        scheduledExecutor.scheduleWithFixedDelay(this::refreshAll, intervalMillis, intervalMillis, MILLISECONDS);
    }

    void register(URI workerUri, String nodeInstanceId, TaskId taskId, ContinuousTaskStatusFetcher fetcher)
    {
        workers.compute(new Worker(workerUri, nodeInstanceId), (_, tasks) -> {
            WorkerTasks workerTasks = tasks;
            if (workerTasks == null) {
                workerTasks = new WorkerTasks();
            }
            workerTasks.fetchers.put(taskId, fetcher);
            return workerTasks;
        });
    }

    void unregister(URI workerUri, String nodeInstanceId, TaskId taskId)
    {
        WorkerTasks workerTasks = workers.get(new Worker(workerUri, nodeInstanceId));
        if (workerTasks != null) {
            workerTasks.fetchers.remove(taskId);
        }
    }

    private void refreshAll()
    {
        try {
            for (Worker worker : workers.keySet()) {
                // drop workers without registered tasks; registration recreates them
                WorkerTasks workerTasks = workers.computeIfPresent(worker, (_, tasks) -> {
                    if (tasks.fetchers.isEmpty()) {
                        return null;
                    }
                    return tasks;
                });
                if (workerTasks != null && workerTasks.requestInFlight.compareAndSet(false, true)) {
                    refresh(worker, workerTasks);
                }
            }
        }
        catch (Throwable t) {
            // an exception would cancel the periodic refresh
            log.error(t, "Error refreshing worker task status");
        }
    }

    private void refresh(Worker worker, WorkerTasks workerTasks)
    {
        Request request = prepareGet()
                .setUri(uriBuilderFrom(worker.uri()).appendPath("/v1/task/status").build())
                .setHeader(TRINO_NODE_INSTANCE_ID_HEADER, worker.nodeInstanceId())
                .build();
        ListenableFuture<JsonResponse<List<TaskStatus>>> future;
        try {
            future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskStatusListCodec));
        }
        catch (RuntimeException e) {
            workerTasks.requestInFlight.set(false);
            throw e;
        }
        Futures.addCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(JsonResponse<List<TaskStatus>> response)
            {
                try {
                    if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                        for (TaskStatus taskStatus : response.getValue()) {
                            ContinuousTaskStatusFetcher fetcher = workerTasks.fetchers.get(taskStatus.taskId());
                            if (fetcher != null) {
                                fetcher.updateTaskStatus(taskStatus);
                            }
                        }
                    }
                }
                finally {
                    workerTasks.requestInFlight.set(false);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                // per-task status requests track errors; the next interval retries
                workerTasks.requestInFlight.set(false);
            }
        }, executor);
    }

    private record Worker(URI uri, String nodeInstanceId) {}

    private static class WorkerTasks
    {
        private final Map<TaskId, ContinuousTaskStatusFetcher> fetchers = new ConcurrentHashMap<>();
        private final AtomicBoolean requestInFlight = new AtomicBoolean();
    }
}
