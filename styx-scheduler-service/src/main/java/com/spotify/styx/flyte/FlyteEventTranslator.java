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

import static com.spotify.styx.state.RunState.MISSING_DEPS_EXIT_CODE;
import static com.spotify.styx.state.RunState.SUCCESS_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Event;
import com.spotify.styx.state.RunState;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteEventTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(FlyteEventTranslator.class);
  private static final String RETRIES_EXHAUSTED = "RetriesExhausted|";
  private static final String PERSISTED_CODE = "USER:Persisted";
  private static final String NOT_RETRYABLE_CODE = "USER:NotRetryable";
  private static final String NOT_READY_CODE = "USER:NotReady";

  private static final Map<String, Integer> KNOWN_ERROR_CODES = Map.of(
      PERSISTED_CODE, SUCCESS_EXIT_CODE, // lookup task throw this to signal that data was produced
      NOT_READY_CODE, MISSING_DEPS_EXIT_CODE,
      NOT_RETRYABLE_CODE, UNRECOVERABLE_FAILURE_EXIT_CODE
  );

  private FlyteEventTranslator() {
    throw new UnsupportedOperationException();
  }

  static List<Event> translate(ExecutionOuterClass.Execution execution, RunState runState) {
    final Execution.WorkflowExecution.Phase phase = execution.getClosure().getPhase();
    final FlytePhase flytePhase = FlytePhase.fromProto(phase);
    if (isExited(flytePhase)) {
      return handleExited(execution, runState, flytePhase);
    }

    if (isStarted(flytePhase)) {
      return handleStarted(flytePhase, runState);
    }
    return List.of();
  }

  static private boolean isExited(FlytePhase flytePhase) {
    switch (flytePhase) {
      case SUCCEEDED:
      case FAILED:
      case ABORTED:
      case TIMED_OUT:
        return true;
    }
    return false;
  }

  static private boolean isStarted(FlytePhase flytePhase) {
    return flytePhase == FlytePhase.RUNNING;
  }

  static private List<Event> handleExited(ExecutionOuterClass.Execution execution, RunState runState,
                                     FlytePhase flytePhase) {
    List<Event> generatedEvents = Lists.newArrayList();
    if (runState.state() ==  RunState.State.SUBMITTED) {
      generatedEvents.add(Event.started(runState.workflowInstance()));
    }

    switch (flytePhase) {
      case SUCCEEDED:
        LOG.info("Issue 'terminate' event for: " + runState.workflowInstance());
        generatedEvents.add(Event.terminate(runState.workflowInstance(), Optional.of(SUCCESS_EXIT_CODE)));
        break;
      case FAILED:
      case ABORTED:
      case TIMED_OUT:
        final String flyteCode = execution.getClosure().getError().getCode().replace(RETRIES_EXHAUSTED,"");
        final int styxCode = KNOWN_ERROR_CODES.getOrDefault(flyteCode, UNKNOWN_ERROR_EXIT_CODE);
        LOG.info("Issue 'terminate' event for: " + runState.workflowInstance());
        generatedEvents.add(Event.terminate(runState.workflowInstance(), Optional.of(styxCode)));
        break;
    }
    return generatedEvents;
  }

  static private List<Event> handleStarted(FlytePhase flytePhase, RunState runState) {
    if (flytePhase == FlytePhase.RUNNING && runState.state() == RunState.State.SUBMITTED) {
      LOG.info("Issue 'started' event for: " + runState.workflowInstance());
      return List.of(Event.started(runState.workflowInstance()));
    }

    return List.of();
  }
}
