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

import static com.github.rholder.retry.StopStrategies.stopAfterDelay;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.spotify.styx.ScheduledExecutionUtil.scheduleWithJitter;
import static com.spotify.styx.flyte.FlyteEventTranslator.translate;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.GrpcContextUtil.currentContextExecutorService;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.styx.docker.LabelValue;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.flyte.client.FlyteInputsUtils;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.TriggerVisitor;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClientRunner implements FlyteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClientRunner.class);

  @VisibleForTesting
  static final String TERMINATE_CAUSE_PREFIX = "Styx workflow instance execution reached state: ";

  @VisibleForTesting
  static final String TERMINATE_CAUSE = "Styx workflow instance execution is not active";

  @VisibleForTesting
  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";

  @VisibleForTesting static final String STYX_EXECUTION_ID_ANNOTATION = "styx-execution-id";
  @VisibleForTesting static final Duration TERMINATION_GRACE_PERIOD = Duration.ofMinutes(3);
  private static final int FLYTE_TERMINATING_THREADS = 4; // TODO: tune
  private static final Duration DEFAULT_TERMINATE_EXEC_INTERVAL = Duration.ofMinutes(1);
  private static final ThreadFactory THREAD_FACTORY =
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("flyte-scheduler-thread-%d").build();
  private static final String STYX_COMPONENT_ID = "STYX_COMPONENT_ID";
  private static final String STYX_WORKFLOW_ID = "STYX_WORKFLOW_ID";
  private static final String STYX_PARAMETER = "STYX_PARAMETER";
  private static final String STYX_EXECUTION_ID = "STYX_EXECUTION_ID";
  private static final String STYX_TRIGGER_ID = "STYX_TRIGGER_ID";
  private static final String STYX_TRIGGER_TYPE = "STYX_TRIGGER_TYPE";

  private final String runnerId;
  private final FlyteAdminClient flyteAdminClient;
  private final StateManager stateManager;
  private final Duration terminateDanglingFlyteExecInterval;
  private final ScheduledExecutorService scheduledExecutor;
  private final Time time;
  private final Closer closer = Closer.create();
  private final ExecutorService executor;

  @VisibleForTesting
  FlyteAdminClientRunner(
      final String runnerId,
      final FlyteAdminClient flyteAdminClient,
      final StateManager stateManager,
      final Duration terminateDanglingFlyteExecInterval,
      final ScheduledExecutorService scheduledExecutor,
      final Time time) {
    this.runnerId = requireNonNull(runnerId, "runnerId");
    this.flyteAdminClient = requireNonNull(flyteAdminClient, "flyteAdminClient");
    this.stateManager = requireNonNull(stateManager, "stateManager");
    this.terminateDanglingFlyteExecInterval =
        requireNonNull(terminateDanglingFlyteExecInterval, "terminateFlyteExecInterval");
    this.scheduledExecutor =
        register(
            closer,
            requireNonNull(scheduledExecutor, "flyte-scheduled-executor"),
            "flyte-scheduled-executor");
    this.time = requireNonNull(time, "time");

    checkArgument(
        terminateDanglingFlyteExecInterval.compareTo(Duration.ZERO) > 0,
        "terminateFlyteExecInterval must be greater than zero:"
            + terminateDanglingFlyteExecInterval);

    this.executor =
        currentContextExecutorService(
            register(
                closer, new ForkJoinPool(FLYTE_TERMINATING_THREADS), "flyte-terminating-executor"));
  }

  FlyteAdminClientRunner(
      final String runnerId,
      final FlyteAdminClient flyteAdminClient,
      final StateManager stateManager) {
    this(
        runnerId,
        flyteAdminClient,
        stateManager,
        DEFAULT_TERMINATE_EXEC_INTERVAL,
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY),
        Instant::now);
  }

  @Override
  public String createExecution(
      final RunState runState, final String execName, final FlyteExecConf flyteExecConf)
      throws CreateExecutionException {
    requireNonNull(runState, "runState");
    requireNonNull(execName, "name");
    requireNonNull(flyteExecConf, "flyteExecConf");
    final var launchPlanIdentifier = flyteExecConf.referenceId();
    final var execMode =
        runState
            .data()
            .trigger()
            .map(this::toFlyteExecutionMode)
            .filter(mode -> mode != ExecutionMode.UNRECOGNIZED)
            .orElseThrow(
                () ->
                    new CreateExecutionException(
                        "Missing trigger or unknown in StateData: " + runState.data()));

    final var styxVariables = getStyxVariables(runState.workflowInstance(), runState.data());
    final var triggeredParams =
        runState
            .data()
            .triggerParameters()
            .map(FlyteAdminClientRunner::getFilteredTriggerParams)
            .orElseGet(Map::of);

    final var labels =
        styxVariables.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), LabelValue.normalize(entry.getValue())))
            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    final var annotations =
        ImmutableMap.<String, String>builder()
            .putAll(styxVariables)
            // just to keep compatibility with removing dangling executions
            // TODO: base dangling logic on STYX_WORKFLOW_ID, STYX_PARAMETER and STYX_EXECUTION_ID
            //   and the remove these annotations
            .put(STYX_WORKFLOW_INSTANCE_ANNOTATION, runState.workflowInstance().toKey())
            .put(STYX_EXECUTION_ID_ANNOTATION, styxVariables.get(STYX_EXECUTION_ID))
            .build();

    // First use the fields stored in the flyteExecConf
    // Then override with the triggeredParams
    var userDefinedInputs =
        FlyteInputsUtils.combineMapsCaseInsensitiveWithOrder(
            flyteExecConf.inputFields(), triggeredParams);

    try {
      retry(
          () ->
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
                  /* labels = */ labels,
                  /* annotations = */ annotations,
                  /* extraDefaultInputs = */ userDefinedInputs,
                  /* styxVariables */ styxVariables));
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

  private static <T> T retry(Callable<T> callable) throws ExecutionException, RetryException {
    var retryer =
        RetryerBuilder.<T>newBuilder()
            .retryIfException(FlyteAdminClientRunner::isRetryableException)
            .withWaitStrategy(exponentialWait())
            .withStopStrategy(stopAfterDelay(30, SECONDS))
            .build();
    return retryer.call(callable);
  }

  private static boolean isRetryableException(Throwable t) {
    LOG.info("Class: " + t.getClass());
    if (t instanceof StatusRuntimeException) {
      final StatusRuntimeException s = (StatusRuntimeException) t;
      LOG.info("Code: " + s.getStatus().getCode());
      LOG.info("Status: " + s.getStatus());
      LOG.info("Cause: " + s.getCause());
      LOG.info("Message: " + s.getMessage());
      if (s.getStatus().getCode() == Status.Code.INTERNAL) {
        if (s.getCause() instanceof GoogleJsonResponseException) {
          return ((GoogleJsonResponseException) s.getCause()).getStatusCode() == 409;
        }
      }
    }
    return false;
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
  public void poll(FlyteExecutionId flyteExecutionId, RunState runState) {
    requireNonNull(flyteExecutionId, "flyteExecutionId");
    requireNonNull(runState, "runState");
    final ExecutionOuterClass.Execution execution;
    try {
      execution =
          flyteAdminClient.getExecution(
              flyteExecutionId.project(), flyteExecutionId.domain(), flyteExecutionId.name());
    } catch (StatusRuntimeException e) {
      LOG.warn("Failed to poll flyte execution {}", flyteExecutionId, e);

      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        stateManager.receiveIgnoreClosed(
            Event.runError(
                runState.workflowInstance(),
                "Could not find execution: " + flyteExecutionId.toUrn()),
            runState.counter());
      }

      return;
    }

    emitFlyteEvents(execution, runState);
  }

  void init() {
    LOG.info("Scheduling terminate dangling flyte execution thread");
    scheduleWithJitter(
        this::terminateDanglingFlyteExecutions,
        scheduledExecutor,
        terminateDanglingFlyteExecInterval);
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
          // TODO: explore using filters for listing only running executions
          // or at least listing only the ones newer than some threshold
          var executions =
              flyteAdminClient.listExecutions(
                  project.getId(), domain.getId(), 100, paginationToken, "");
          executions.getExecutionsList().stream()
              .filter(this::haveBeenRunningForAWhile)
              .map(
                  exec ->
                      AnnotatedFlyteExecutionId.create(
                          FlyteExecutionId.fromProto(exec.getId()),
                          exec.getSpec().getAnnotations().getValuesMap()))
              .filter(this::isFlyteExecutionTriggeredByStyx)
              .map(
                  annotatedId ->
                      runAsync(
                          guard(() -> tryTerminateDanglingFlyteExecution(annotatedId)), executor))
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
      LOG.info(
          "Skipping termination on young running Flyte execution: ex:{}::{}:{} ({})",
          exec.getId().getProject(),
          exec.getId().getDomain(),
          exec.getId().getName(),
          age);
      return false;
    }
    return true;
  }

  private void tryTerminateDanglingFlyteExecution(AnnotatedFlyteExecutionId annotatedId) {
    var workflowInstance = getWorkflowInstance(annotatedId);
    var runState = stateManager.getActiveState(workflowInstance);

    var shouldTerminate =
        runState.isEmpty() // state reached terminal state and aren't  active anymore
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
    checkArgument(
        isFlyteExecutionTriggeredByStyx(annotatedId),
        "Flyte execution is not triggered by styx. annotatedId: " + annotatedId);

    return WorkflowInstance.parseKey(
        annotatedId.annotation().get(STYX_WORKFLOW_INSTANCE_ANNOTATION));
  }

  private boolean isFlyteExecRelatedToRunState(
      AnnotatedFlyteExecutionId annotatedId, RunState runState) {
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
      LOG.info(
          "Flyte execution not matching current exec id, current:{} != flyte:{}",
          styxExecId,
          styxExecIdOnFlyte);
      return false;
    }

    return true;
  }

  private static Map<String, String> getStyxVariables(
      WorkflowInstance workflowInstance, StateData stateData) {
    var triggerType = stateData.trigger().map(TriggerUtil::triggerType);
    var triggerId = stateData.trigger().map(TriggerUtil::triggerId);

    var mapBuilder = ImmutableMap.<String, String>builder();

    mapBuilder.put(STYX_COMPONENT_ID, workflowInstance.workflowId().componentId());
    mapBuilder.put(STYX_WORKFLOW_ID, workflowInstance.workflowId().id());
    mapBuilder.put(STYX_PARAMETER, workflowInstance.parameter());
    stateData.executionId().ifPresent(value -> mapBuilder.put(STYX_EXECUTION_ID, value));
    triggerId.ifPresent(value -> mapBuilder.put(STYX_TRIGGER_ID, value));
    triggerType.ifPresent(value -> mapBuilder.put(STYX_TRIGGER_TYPE, value));

    return mapBuilder.build();
  }

  // returns trigger parameters but avoid any STYX_XXX ones to prevent collisions
  private static Map<String, String> getFilteredTriggerParams(TriggerParameters triggerParameters) {
    return triggerParameters.env().entrySet().stream()
        .filter(entry -> !entry.getKey().toLowerCase(Locale.ROOT).startsWith("styx_"))
        .collect(toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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
    // TODO: Make FlyteAdminClient closable or use apollo modules to close its channel
    closer.close();
  }

  private static class TriggerToExecutionModeVisitor implements TriggerVisitor<ExecutionMode> {
    private static final TriggerToExecutionModeVisitor INSTANCE =
        new TriggerToExecutionModeVisitor();

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
      // Backfills in Styx doesn't provide idempotency guaranties so ExecutionMode.RELAUNCH doesn't
      // apply
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

    static AnnotatedFlyteExecutionId create(
        FlyteExecutionId identifier, Map<String, String> annotations) {
      return new AnnotatedFlyteExecutionIdBuilder()
          .identifier(identifier)
          .annotation(annotations)
          .build();
    }
  }
}
