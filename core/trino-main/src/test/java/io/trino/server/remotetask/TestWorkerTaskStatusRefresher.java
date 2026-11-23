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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.DynamicFiltersCollector.VersionedDynamicFilterDomains;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.server.DynamicFilterService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.execution.TaskState.PLANNED;
import static io.trino.execution.TaskState.RUNNING;
import static io.trino.execution.TaskStatus.STARTING_VERSION;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.server.InternalHeaders.TRINO_NODE_INSTANCE_ID_HEADER;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.lang.Thread.onSpinWait;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkerTaskStatusRefresher
{
    private static final URI WORKER_URI = URI.create("http://worker.invalid:8080");
    private static final String NODE_INSTANCE_ID = "node-instance";
    private static final TaskId TASK_ID = new TaskId(new StageId("test", 1), 0, 0);
    private static final TaskId OTHER_TASK_ID = new TaskId(new StageId("test", 1), 1, 0);
    private static final JsonCodec<List<TaskStatus>> TASK_STATUS_LIST_CODEC = JsonCodec.listJsonCodec(TaskStatus.class);

    @Test
    @Timeout(10)
    public void testRefreshUpdatesRegisteredTasks()
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<>();
            AtomicReference<List<TaskStatus>> workerStatuses = new AtomicReference<>(ImmutableList.of(
                    createTaskStatus(TASK_ID, 1, RUNNING, 5),
                    createTaskStatus(OTHER_TASK_ID, 1, RUNNING, 7)));
            TestingHttpClient httpClient = new TestingHttpClient(request -> {
                requests.add(request.getUri().getPath() + " " + request.getHeader(TRINO_NODE_INSTANCE_ID_HEADER));
                return new TestingResponse(
                        HttpStatus.OK,
                        ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                        TASK_STATUS_LIST_CODEC.toJsonBytes(workerStatuses.get()));
            });
            WorkerTaskStatusRefresher refresher = new WorkerTaskStatusRefresher(httpClient, TASK_STATUS_LIST_CODEC, executor, executor, new Duration(10, MILLISECONDS));
            ContinuousTaskStatusFetcher fetcher = createStatusFetcher(refresher, executor);
            refresher.register(WORKER_URI, NODE_INSTANCE_ID, TASK_ID, fetcher);
            refresher.start();

            while (fetcher.getTaskStatus().queuedPartitionedDrivers() != 5) {
                onSpinWait();
            }
            assertThat(fetcher.getTaskStatus().state()).isEqualTo(RUNNING);
            assertThat(requests).contains("/v1/task/status " + NODE_INSTANCE_ID);

            // counters refresh without a version change
            workerStatuses.set(ImmutableList.of(createTaskStatus(TASK_ID, 1, RUNNING, 3)));
            while (fetcher.getTaskStatus().queuedPartitionedDrivers() != 3) {
                onSpinWait();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testUnregisteredTaskIsNotUpdated()
            throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        try {
            ConcurrentLinkedQueue<String> requests = new ConcurrentLinkedQueue<>();
            TestingHttpClient httpClient = new TestingHttpClient(request -> {
                requests.add(request.getUri().getPath());
                return new TestingResponse(
                        HttpStatus.OK,
                        ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString()),
                        TASK_STATUS_LIST_CODEC.toJsonBytes(ImmutableList.of(createTaskStatus(TASK_ID, 1, RUNNING, 5))));
            });
            WorkerTaskStatusRefresher refresher = new WorkerTaskStatusRefresher(httpClient, TASK_STATUS_LIST_CODEC, executor, executor, new Duration(10, MILLISECONDS));
            ContinuousTaskStatusFetcher fetcher = createStatusFetcher(refresher, executor);
            refresher.register(WORKER_URI, NODE_INSTANCE_ID, TASK_ID, fetcher);
            refresher.unregister(WORKER_URI, NODE_INSTANCE_ID, TASK_ID);
            refresher.start();

            SECONDS.sleep(1);
            assertThat(requests).isEmpty();
            assertThat(fetcher.getTaskStatus().state()).isEqualTo(PLANNED);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static ContinuousTaskStatusFetcher createStatusFetcher(WorkerTaskStatusRefresher refresher, ScheduledExecutorService executor)
    {
        TestingHttpClient unusedHttpClient = new TestingHttpClient(_ -> { throw new UnsupportedOperationException(); });
        DynamicFiltersFetcher dynamicFiltersFetcher = new DynamicFiltersFetcher(
                _ -> {},
                TASK_ID,
                taskUri(TASK_ID),
                NODE_INSTANCE_ID,
                new Duration(10, SECONDS),
                JsonCodec.jsonCodec(VersionedDynamicFilterDomains.class),
                executor,
                unusedHttpClient,
                () -> noopTracer().spanBuilder("test"),
                new Duration(10, SECONDS),
                executor,
                new RemoteTaskStats(),
                new DynamicFilterService(
                        createTestingMetadataManager(),
                        PLANNER_CONTEXT.getFunctionManager(),
                        PLANNER_CONTEXT.getTypeManager(),
                        new DynamicFilterConfig()));

        return new ContinuousTaskStatusFetcher(
                _ -> {},
                createTaskStatus(TASK_ID, STARTING_VERSION, PLANNED, 0),
                WORKER_URI,
                NODE_INSTANCE_ID,
                new Duration(10, SECONDS),
                refresher,
                JsonCodec.jsonCodec(TaskStatus.class),
                dynamicFiltersFetcher,
                executor,
                unusedHttpClient,
                () -> noopTracer().spanBuilder("test"),
                new Duration(10, SECONDS),
                executor,
                new RemoteTaskStats());
    }

    private static TaskStatus createTaskStatus(TaskId taskId, long version, TaskState state, int queuedPartitionedDrivers)
    {
        return new TaskStatus(
                taskId,
                42,
                version,
                state,
                taskUri(taskId),
                "node-id",
                false,
                ImmutableList.of(),
                queuedPartitionedDrivers,
                0,
                OutputBufferStatus.initial(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                OptionalInt.empty(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                0,
                0,
                0);
    }

    private static URI taskUri(TaskId taskId)
    {
        return WORKER_URI.resolve("/v1/task/" + taskId);
    }
}
