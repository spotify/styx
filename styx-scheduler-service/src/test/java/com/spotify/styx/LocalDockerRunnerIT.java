/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
 * --
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
 * -/-/-
 */

package com.spotify.styx;

import static com.spotify.styx.model.ExecutionStatus.FAILED;
import static com.spotify.styx.model.ExecutionStatus.MISSING_DEPS;
import static com.spotify.styx.model.ExecutionStatus.STARTED;
import static com.spotify.styx.model.ExecutionStatus.SUCCEEDED;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.ExecutionStatus;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.handlers.DockerRunnerHandler;
import com.spotify.styx.state.handlers.ExecutionDescriptionHandler;
import com.spotify.styx.state.handlers.StorageHandler;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.workflow.ParameterUtil;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Depends on Docker running locally.
 */
public class LocalDockerRunnerIT {

  private List<ExecutionStatus> getExecutionStatuses(int returnCode, int expectedStores)
      throws InterruptedException, ExecutionException, IOException {

    final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    final StateManager stateManager = new SyncStateManager();
    final DockerRunner runner = DockerRunner.local(exec, stateManager);
    final InMemStorage recorder = new InMemStorage(expectedStores);

    final StateInitializingTrigger stateInitializingTrigger =
        new StateInitializingTrigger((workflowInstance) ->
            RunState.fresh(
                workflowInstance,
                new ExecutionDescriptionHandler(recorder, stateManager),
                new DockerRunnerHandler(runner, stateManager),
                new StorageHandler(recorder, Clock.systemUTC()),
                new TerminationHandler(Duration.ofMillis(100), 1, stateManager)
            ), stateManager, recorder);

    final Instant now = Instant.now();
    final Workflow workflow = Workflow.create(
        "example",
        URI.create("http://example.com"),
        DataEndpoint.create(
            "example",
            Partitioning.HOURS,
            Optional.of("busybox"),
            Optional.of(ImmutableList.of("/bin/sh", "-c", "exit " + returnCode)),
            empty()
        )
    );
    recorder.store(workflow);
    stateInitializingTrigger.event(workflow, "test-trigger", now);

    final String parameter = ParameterUtil.formatDateHour(now);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), parameter);

    recorder.countDown.await(10, TimeUnit.SECONDS);
    return recorder.getStoredStatuses(workflowInstance);
  }

  @Test
  public void shouldHandleSucceededStatus() throws Exception {
    assertThat(
        getExecutionStatuses(0, 2),
        contains(STARTED, SUCCEEDED));
  }

  @Test
  public void shouldHandleFailedStatus() throws Exception {
    assertThat(
        getExecutionStatuses(1, 2),
        contains(STARTED, FAILED));
    assertThat(
        getExecutionStatuses(21, 2),
        contains(STARTED, FAILED));
    assertThat(
        getExecutionStatuses(255, 2),
        contains(STARTED, FAILED));
  }

  @Test
  public void shouldHandleMissingDepsStatus() throws Exception {
    assertThat(
        getExecutionStatuses(20, 2),
        contains(STARTED, MISSING_DEPS));
  }
}
