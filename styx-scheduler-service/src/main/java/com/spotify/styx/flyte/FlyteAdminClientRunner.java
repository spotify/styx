/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import static com.google.common.base.Preconditions.checkArgument;
import static com.spotify.styx.ScheduledExecutionUtil.scheduleWithJitter;
import static com.spotify.styx.flyte.FlyteEventTranslator.translate;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.GrpcContextUtil.currentContextExecutorService;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.TriggerVisitor;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.norberg.automatter.AutoMatter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClientRunner implements FlyteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClientRunner.class);
  @VisibleForTesting static final String TERMINATE_CAUSE_PREFIX = "Styx workflow instance execution reached state: ";
  @VisibleForTesting static final String TERMINATE_CAUSE = "Styx workflow instance execution is not active";
  @VisibleForTesting static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  @VisibleForTesting static final String STYX_EXECUTION_ID_ANNOTATION = "styx-execution-id";
  @VisibleForTesting static final Duration TERMINATION_GRACE_PERIOD = Duration.ofMinutes(3);
  private static final int FLYTE_TERMINATING_THREADS = 4; //TODO: tune
  private static final Duration DEFAULT_TERMINATE_EXEC_INTERVAL = Duration.ofMinutes(1);
  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("flyte-scheduler-thread-%d")
      .build();

  private final String runnerId;
  private final FlyteAdminClient flyteAdminClient;
  private final StateManager stateManager;
  private final Duration terminateDanglingFlyteExecInterval;
  private final ScheduledExecutorService scheduledExecutor;
  private final Time time;
  private final Closer closer = Closer.create();
  private final ExecutorService executor;

  @VisibleForTesting
  FlyteAdminClientRunner(final String runnerId,
                         final FlyteAdminClient flyteAdminClient,
                         final StateManager stateManager,
                         final Duration terminateDanglingFlyteExecInterval,
                         final ScheduledExecutorService scheduledExecutor,
                         final Time time) {
    this.runnerId = requireNonNull(runnerId, "runnerId");
    this.flyteAdminClient = requireNonNull(flyteAdminClient, "flyteAdminClient");
    this.stateManager = requireNonNull(stateManager, "stateManager");
    this.terminateDanglingFlyteExecInterval = requireNonNull(terminateDanglingFlyteExecInterval, "terminateFlyteExecInterval");
    this.scheduledExecutor = register(closer, requireNonNull(
        scheduledExecutor, "flyte-scheduled-executor"),
        "flyte-scheduled-executor"
    );
    this.time = requireNonNull(time, "time");

    checkArgument(
        terminateDanglingFlyteExecInterval.compareTo(Duration.ZERO) > 0,
        "terminateFlyteExecInterval must be greater than zero:" + terminateDanglingFlyteExecInterval);

    this.executor = currentContextExecutorService(
        register(closer, new ForkJoinPool(FLYTE_TERMINATING_THREADS), "flyte-terminating-executor"));
  }

  FlyteAdminClientRunner(final String runnerId,
                         final FlyteAdminClient flyteAdminClient,
                         final StateManager stateManager) {
    this(runnerId, flyteAdminClient, stateManager, 
        DEFAULT_TERMINATE_EXEC_INTERVAL,
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY),
        Instant::now);
  }

  @Override
  public String createExecution(final RunState runState, final String execName, final FlyteExecConf flyteExecConf,
                                final Map<String, String> annotations)
      throws CreateExecutionException {
    requireNonNull(runState, "runState");
    requireNonNull(execName, "name");
    requireNonNull(flyteExecConf, "flyteExecConf");
    requireNonNull(annotations, "annotations");
    final var launchPlanIdentifier = flyteExecConf.referenceId();
    final var execMode = runState.data().trigger()
        .map(this::toFlyteExecutionMode)
        .filter(mode -> mode != ExecutionMode.UNRECOGNIZED)
        .orElseThrow(() -> new CreateExecutionException("Missing trigger or unknown in StateData: " + runState.data()));

    final var parameter = runState.workflowInstance().parameter();

    try {
      flyteAdminClient.createExecution(
          launchPlanIdentifier.project(),
          launchPlanIdentifier.domain(),
          execName,
          IdentifierOuterClass.Identifier.newBuilder()
              .setName(launchPlanIdentifier.name())
              .setProject(launchPlanIdentifier.project())
              .setDomain(launchPlanIdentifier.domain())
              .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
              .setVersion(launchPlanIdentifier.version())
              .build(),
          execMode,
          annotations,
          parameter);
      return runnerId;
    } catch (StatusRuntimeException e) {
      switch (e.getStatus().getCode()) {
        case ALREADY_EXISTS:
          // TODO: 🤔 How do we make sure that we tell apart between low probabilities of collisions
          //  on hashing of styx-run-id --> flyte exec and repetitions of same styx-id execution.
          //  For now we always assume the later.
          return runnerId;
        case NOT_FOUND:
          throw new LaunchPlanNotFound(flyteExecConf, e);
      }
      throw new CreateExecutionException(flyteExecConf, e);
    } catch (Exception e) {
      throw new CreateExecutionException(flyteExecConf, e);
    }
  }

  @Override
  public void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId) {
    requireNonNull(runState, "runState");
    requireNonNull(flyteExecutionId, "flyteExecutionId");
    try {
      // Flyte admin tolerates terminate request over an already terminated workflow execution
      // so no need to check that workflow execution hasn't been terminated yet
      flyteAdminClient.terminateExecution(
          flyteExecutionId.project(),
          flyteExecutionId.domain(),
          flyteExecutionId.name(),
          getCause(runState));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        LOG.warn("Trying to terminate non existent flyte execution: {}", flyteExecutionId);
        return;
      } // TODO: Consider other status codes that could be interesting
      LOG.error("Couldn't terminate flyte execution: {}", flyteExecutionId, e);
    } catch (Exception e) {
      LOG.error("Couldn't terminate flyte execution: {}", flyteExecutionId, e);
    }
  }

  private String getCause(RunState runState) {
    return TERMINATE_CAUSE_PREFIX + runState.state();
  }

  @Override
  public void poll(FlyteExecutionId flyteExecutionId, RunState runState)
      throws PollingException {
    requireNonNull(flyteExecutionId, "flyteExecutionId");
    requireNonNull(runState, "runState");
    try {
      final ExecutionOuterClass.Execution execution =
          flyteAdminClient.getExecution(flyteExecutionId.project(),
              flyteExecutionId.domain(), flyteExecutionId.name());
      emitFlyteEvents(execution, runState);
    } catch (StatusRuntimeException e) {
      LOG.warn("Failed to poll flyte execution {}", flyteExecutionId, e);
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new ExecutionNotFoundException(flyteExecutionId, e);
      }
      throw new PollingException(flyteExecutionId, e);
    } catch (Exception e) {
      throw new PollingException(flyteExecutionId, e);
    }
  }

  void init() {
    LOG.info("Scheduling terminate dangling flyte execution thread");
    scheduleWithJitter(this::terminateDanglingFlyteExecutions, scheduledExecutor, terminateDanglingFlyteExecInterval);
  }

  @VisibleForTesting
  void terminateDanglingFlyteExecutions() {
    try {
      // TODO: add tracing
      tryTerminateDanglingFlyteExecutions();
    } catch (Throwable t) {
      LOG.error("Error while terminating dangling flyte executions", t);
    }

  }

  private void tryTerminateDanglingFlyteExecutions() {
    var projects = flyteAdminClient.listProjects();

    var allFutures = new ArrayList<CompletableFuture<Void>>();
    for (var project : projects.getProjectsList()) {
      for (var domain : project.getDomainsList()) {
        String paginationToken = null;
        do {
          //TODO: explore using filters for listing only running executions
          // or at least listing only the ones newer than some threshold
          var executions =
              flyteAdminClient.listExecutions(project.getId(), domain.getId(), 100, paginationToken, "");
          executions.getExecutionsList().stream()
              .filter(this::haveBeenRunningForAWhile)
              .map(exec -> AnnotatedFlyteExecutionId.create(
                  FlyteExecutionId.fromProto(exec.getId()),
                  exec.getSpec().getAnnotations().getValuesMap())
              )
              .filter(this::isFlyteExecutionTriggeredByStyx)
              .map(annotatedId -> runAsync(guard(() -> tryTerminateDanglingFlyteExecution(annotatedId)), executor))
              .forEach(allFutures::add);
          paginationToken = executions.getToken();
        } while (!"".equals(paginationToken));
      }
    }
    allFutures.forEach(CompletableFuture::join);
  }

  private boolean haveBeenRunningForAWhile(ExecutionOuterClass.Execution exec) {
    var isRunning = exec.getClosure().getPhase() == Execution.WorkflowExecution.Phase.RUNNING;
    if (!isRunning) {
      return false;
    }

    var startedAt = exec.getClosure().getStartedAt();
    var startedAtInstant = Instant.ofEpochSecond(startedAt.getSeconds(), startedAt.getNanos());
    var age = Duration.between(startedAtInstant, time.get());
    if (age.compareTo(TERMINATION_GRACE_PERIOD) < 0) {
      LOG.info("Skipping termination on young running Flyte execution: ex:{}::{}:{} ({})",
          exec.getId().getProject(), exec.getId().getDomain(), exec.getId().getName(), age);
      return false;
    }
    return true;
  }

  private void tryTerminateDanglingFlyteExecution(AnnotatedFlyteExecutionId annotatedId) {
    var workflowInstance = getWorkflowInstance(annotatedId);
    var runState = stateManager.getActiveState(workflowInstance);

    var shouldTerminate = runState.isEmpty() // state reached terminal state and aren't  active anymore
                          || !isFlyteExecRelatedToRunState(annotatedId, runState.orElseThrow());
    if (shouldTerminate) {
      var id = annotatedId.identifier();
      flyteAdminClient.terminateExecution(id.project(), id.domain(), id.name(), TERMINATE_CAUSE);
    }
  }

  private boolean isFlyteExecutionTriggeredByStyx(AnnotatedFlyteExecutionId annotatedId) {
    final var annotations = annotatedId.annotation();
    return annotations != null && annotations.containsKey(STYX_WORKFLOW_INSTANCE_ANNOTATION);
  }

  private WorkflowInstance getWorkflowInstance(AnnotatedFlyteExecutionId annotatedId) {
    checkArgument(isFlyteExecutionTriggeredByStyx(annotatedId),
        "Flyte execution is not triggered by styx. annotatedId: " + annotatedId);

    return WorkflowInstance.parseKey(
        annotatedId.annotation().get(STYX_WORKFLOW_INSTANCE_ANNOTATION));
  }

  private boolean isFlyteExecRelatedToRunState(AnnotatedFlyteExecutionId annotatedId, RunState runState) {
    final var annotations = annotatedId.annotation();
    if (!annotations.containsKey(STYX_EXECUTION_ID_ANNOTATION)) {
      LOG.info("Flyte execution without styx execution id annotation {}", annotatedId.identifier());
      return false;
    }
    final var styxExecIdOnFlyte = annotations.get(STYX_EXECUTION_ID_ANNOTATION);

    final Optional<String> styxExecIdOpt = runState.data().executionId();
    if (styxExecIdOpt.isEmpty()) {
      LOG.info("Flyte execution state with no current executionId: {}", styxExecIdOnFlyte);
      return false;
    }

    final String styxExecId = styxExecIdOpt.get();
    if (!styxExecIdOnFlyte.equals(styxExecId)) {
      LOG.info("Flyte execution not matching current exec id, current:{} != flyte:{}",
          styxExecId, styxExecIdOnFlyte);
      return false;
    }

    return true;
  }

  @VisibleForTesting
  void emitFlyteEvents(ExecutionOuterClass.Execution execution, RunState runState) {
    final List<Event> events = translate(execution, runState);
    for (int i = 0; i < events.size(); ++i) {
      final Event event = events.get(i);
      try {
        stateManager.receive(event, runState.counter() + i);
      } catch (StateTransitionConflictException e) {
        LOG.debug("State transition conflict on flyte event: {}", event, e);
        return;
      } catch (CounterCapacityException e) {
        LOG.debug("Counter capacity exhausted when processing flyte event: {}", event, e);
        return;
      } catch (IsClosedException ignore) {
        return;
      }
    }
  }

  private ExecutionMode toFlyteExecutionMode(Trigger trigger) {
    return trigger.accept(TriggerToExecutionModeVisitor.INSTANCE);
  }

  @Override
  public void close() throws IOException {
    //TODO: Make FlyteAdminClient closable or use apollo modules to close its channel
    closer.close();
  }

  private static class TriggerToExecutionModeVisitor implements TriggerVisitor<ExecutionMode> {
    private static final TriggerToExecutionModeVisitor INSTANCE = new TriggerToExecutionModeVisitor();

    @Override
    public ExecutionMode natural() {
      return ExecutionMode.SCHEDULED;
    }

    @Override
    public ExecutionMode adhoc(String triggerId) {
      return ExecutionMode.MANUAL;
    }

    @Override
    public ExecutionMode backfill(String triggerId) {
      // Backfills in Styx doesn't provide idempotency guaranties so ExecutionMode.RELAUNCH doesn't apply
      return ExecutionMode.MANUAL;
    }

    @Override
    public ExecutionMode unknown(String triggerId) {
      return ExecutionMode.UNRECOGNIZED;
    }
  }

  @AutoMatter
  interface AnnotatedFlyteExecutionId {
    FlyteExecutionId identifier();
    Map<String, String> annotation();

    static AnnotatedFlyteExecutionId create(FlyteExecutionId identifier, Map<String, String> annotations) {
      ;return new AnnotatedFlyteExecutionIdBuilder()
          .identifier(identifier)
          .annotation(annotations)
          .build();
    }
  }
}
