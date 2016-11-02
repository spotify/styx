/*
 * -\-\-
 * Spotify Styx Common
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
package com.spotify.styx.state;

import static com.spotify.styx.state.OutputHandler.fanOutput;
import static com.spotify.styx.state.RunState.State.AWAITING_RETRY;
import static com.spotify.styx.state.RunState.State.DONE;
import static com.spotify.styx.state.RunState.State.ERROR;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.PREPARE;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static com.spotify.styx.state.RunState.State.SUBMITTED;
import static com.spotify.styx.state.RunState.State.SUBMITTING;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static java.lang.System.currentTimeMillis;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Optional;

/**
 * State machine for run states.
 *
 * This implements the following Finite State Transducer (Mealy machine) where the inputs are
 * defined by the {@link Event} enum and outputs defined by the methods of {@link OutputHandler}.
 */
@AutoValue
public abstract class RunState {

  public static final int MISSING_DEPS_EXIT_CODE = 20;

  public static final int INITIAL_TRIES = 0;
  public static final int INITIAL_EXIT = -1;
  public static final double INITIAL_RETRY_COST = 0.0;
  public static final double FAILURE_COST = 1.0;
  public static final double MISSING_DEPS_COST = 0.1;

  private final EventVisitor<RunState> visitor = new TransitionVisitor();

  public enum State {
    NEW(false),
    PREPARE(false),
    SUBMITTING(false),
    SUBMITTED(false),
    RUNNING(false),
    TERMINATED(false),
    FAILED(false),
    AWAITING_RETRY(false),
    ERROR(true),
    DONE(true);

    private final boolean terminal;

    State(boolean terminal) {
      this.terminal = terminal;
    }

    public boolean isTerminal() {
      return terminal;
    }
  }

  public abstract WorkflowInstance workflowInstance();
  public abstract State state();
  public abstract long timestamp();
  public abstract int tries();
  public abstract double retryCost();
  public abstract long retryDelayMillis();
  public abstract int lastExit();
  public abstract Optional<String> executionId();
  public abstract Optional<ExecutionDescription> executionDescription();

  abstract Time time();
  abstract OutputHandler outputHandler();

  public static RunState fresh(
      WorkflowInstance workflowInstance,
      Time time,
      OutputHandler outputHandler) {
    return create(workflowInstance, State.NEW, time, outputHandler);
  }

  public static RunState fresh(
      WorkflowInstance workflowInstance,
      Time time,
      OutputHandler... outputHandlers) {
    return fresh(workflowInstance, time, fanOutput(outputHandlers));
  }

  public static RunState fresh(WorkflowInstance workflowInstance, OutputHandler... outputHandlers) {
    return fresh(workflowInstance, Instant::now, fanOutput(outputHandlers));
  }

  public RunState transition(Event event) {
    return event.accept(visitor);
  }

  public RunState withHandlers(OutputHandler[] outputHandlers) {
    return new AutoValue_RunState(
        workflowInstance(), state(), timestamp(), tries(), retryCost(), retryDelayMillis(),
        lastExit(), executionId(), executionDescription(), time(), fanOutput(outputHandlers));
  }

  public RunState withTime(Time time) {
    return new AutoValue_RunState(
        workflowInstance(), state(), timestamp(), tries(), retryCost(), retryDelayMillis(),
        lastExit(), executionId(), executionDescription(), time, outputHandler());
  }

  private RunState state(State state) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries(), retryCost(),
        retryDelayMillis(), lastExit(), executionId(), executionDescription(), time(), outputHandler());
  }

  private RunState state(State state, ExecutionDescription executionDescription) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries(), retryCost(),
        retryDelayMillis(), lastExit(), executionId(), of(executionDescription), time(), outputHandler());
  }

  private RunState state(State state, String executionId) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries(), retryCost(),
        retryDelayMillis(), lastExit(), of(executionId), executionDescription(), time(), outputHandler());
  }

  private RunState state(State state, String executionId, ExecutionDescription executionDescription) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries(), retryCost(),
        retryDelayMillis(), lastExit(), of(executionId), of(executionDescription), time(), outputHandler());
  }

  private RunState state(State state, long retryDelayMillis) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries(), retryCost(),
        retryDelayMillis, lastExit(), executionId(), executionDescription(), time(), outputHandler());
  }

  private RunState state(State state, int tries) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries, retryCost(),
        retryDelayMillis(), lastExit(), executionId(), executionDescription(), time(), outputHandler());
  }

  private RunState state(State state, int tries, double retryCost, int lastExit) {
    return new AutoValue_RunState(
        workflowInstance(), state, time().get().toEpochMilli(), tries, retryCost,
        retryDelayMillis(), lastExit, executionId(), executionDescription(), time(), outputHandler());
  }

  private class TransitionVisitor implements EventVisitor<RunState> {

    @Override
    public RunState timeTrigger(WorkflowInstance workflowInstance) {
      switch (state()) {
        case NEW:
          return state(SUBMITTED); // for backwards compatibility

        default:
          throw illegalTransition("timeTrigger");
      }
    }

    @Override
    public RunState triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      switch (state()) {
        case NEW:
          return state(PREPARE);

        default:
          throw illegalTransition("triggerExecution");
      }
    }

    @Override
    public RunState created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      switch (state()) {
        case PREPARE:
          return state(SUBMITTED, executionId, ExecutionDescription.forImage(dockerImage));

        default:
          throw illegalTransition("created");
      }
    }

    @Override
    public RunState submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      switch (state()) {
        case PREPARE:
          return state(SUBMITTING, executionDescription);

        default:
          throw illegalTransition("submit");
      }
    }

    @Override
    public RunState submitted(WorkflowInstance workflowInstance, String executionId) {
      switch (state()) {
        case SUBMITTING:
          return state(SUBMITTED, executionId);

        default:
          throw illegalTransition("submitted");
      }
    }

    @Override
    public RunState started(WorkflowInstance workflowInstance) {
      switch (state()) {
        case SUBMITTED:
        case PREPARE:
          return state(RUNNING);

        default:
          throw illegalTransition("started");
      }
    }

    @Override
    public RunState terminate(WorkflowInstance workflowInstance, int exitCode) {
      switch (state()) {
        case RUNNING:
          return (exitCode == MISSING_DEPS_EXIT_CODE)
                 ? state(TERMINATED, tries() + 1, retryCost() + MISSING_DEPS_COST, exitCode)
                 : state(TERMINATED, tries() + 1, retryCost() + FAILURE_COST, exitCode);

        default:
          throw illegalTransition("terminate");
      }
    }

    @Override
    public RunState runError(WorkflowInstance workflowInstance, String message) {
      switch (state()) {
        case SUBMITTING:
        case SUBMITTED:
        case RUNNING:
        case PREPARE:
          return state(FAILED, tries() + 1, retryCost() + FAILURE_COST, INITIAL_EXIT);

        default:
          throw illegalTransition("runError");
      }
    }

    @Override
    public RunState success(WorkflowInstance workflowInstance) {
      switch (state()) {
        case TERMINATED:
          return state(DONE);

        default:
          throw illegalTransition("success");
      }
    }

    @Override
    public RunState retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      switch (state()) {
        case TERMINATED:
        case FAILED:
          return state(AWAITING_RETRY, delayMillis);

        default:
          throw illegalTransition("retryAfter");
      }
    }

    @Override
    public RunState retry(WorkflowInstance workflowInstance) {
      switch (state()) {
        case TERMINATED: // for backwards compatibility
        case FAILED:     // for backwards compatibility
        case AWAITING_RETRY:
          return state(PREPARE);

        default:
          throw illegalTransition("retry");
      }
    }

    @Override
    public RunState stop(WorkflowInstance workflowInstance) {
      switch (state()) {
        case TERMINATED:
        case FAILED:
          return state(ERROR);

        default:
          throw illegalTransition("stop");
      }
    }

    @Override
    public RunState timeout(WorkflowInstance workflowInstance) {
      return state(FAILED, tries() + 1);
    }

    @Override
    public RunState halt(WorkflowInstance workflowInstance) {
      return state(ERROR);
    }
  }

  private IllegalStateException illegalTransition(String event) {
    final String key = workflowInstance().toKey();
    return new IllegalStateException(key + " received " + event + " while in " + state());
  }

  // todo: clean constructors

  public static RunState create(WorkflowInstance workflowInstance, State state) {
    return create(workflowInstance, state, INITIAL_TRIES, INITIAL_EXIT, Instant::now, OutputHandler.NOOP);
  }

  public static RunState create(WorkflowInstance workflowInstance, String executionId, State state) {
    return create(workflowInstance, state, INITIAL_TRIES, INITIAL_EXIT, executionId, Instant::now, OutputHandler.NOOP);
  }

  public static RunState newSubmitting(
      WorkflowInstance workflowInstance,
      ExecutionDescription executionDescription) {
    return new AutoValue_RunState(
        workflowInstance, State.SUBMITTING, currentTimeMillis(), INITIAL_TRIES, INITIAL_RETRY_COST,
        0L, INITIAL_EXIT, empty(), of(executionDescription), Instant::now, OutputHandler.NOOP);
  }

  public static RunState newSubmitted(
      WorkflowInstance workflowInstance,
      String execId,
      ExecutionDescription executionDescription) {
    return new AutoValue_RunState(
        workflowInstance, State.SUBMITTED, currentTimeMillis(), INITIAL_TRIES, INITIAL_RETRY_COST,
        0L, INITIAL_EXIT, of(execId), of(executionDescription), Instant::now, OutputHandler.NOOP);
  }

  public static RunState newRunning(
      WorkflowInstance workflowInstance,
      String execId,
      ExecutionDescription executionDescription) {
    return new AutoValue_RunState(
        workflowInstance, State.RUNNING, currentTimeMillis(), INITIAL_TRIES, INITIAL_RETRY_COST,
        0L, INITIAL_EXIT, of(execId), of(executionDescription), Instant::now, OutputHandler.NOOP);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      Time time,
      OutputHandler outputHandler) {
    return create(workflowInstance, state, INITIAL_TRIES, INITIAL_EXIT, time, outputHandler);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      OutputHandler outputHandler) {
    return new AutoValue_RunState(
        workflowInstance, state, currentTimeMillis(), INITIAL_TRIES, INITIAL_RETRY_COST, 0L,
        INITIAL_EXIT, empty(), empty(), Instant::now, outputHandler);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      int tries,
      int lastExit) {
    return new AutoValue_RunState(
        workflowInstance, state, currentTimeMillis(), tries, INITIAL_RETRY_COST, 0L, lastExit,
        empty(), empty(), Instant::now, OutputHandler.NOOP);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      int tries,
      double retryCost,
      int lastExit,
      OutputHandler outputHandler) {
    return new AutoValue_RunState(
        workflowInstance, state, currentTimeMillis(), tries, retryCost, 0L, lastExit,
        empty(), empty(), Instant::now, outputHandler);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      int tries,
      int lastExit,
      Time time,
      OutputHandler outputHandler) {
    return new AutoValue_RunState(
        workflowInstance, state, time.get().toEpochMilli(), tries, INITIAL_RETRY_COST, 0L, lastExit,
        empty(), empty(), time, outputHandler);
  }

  public static RunState create(
      WorkflowInstance workflowInstance,
      State state,
      int tries,
      int lastExit,
      String executionId,
      Time time,
      OutputHandler outputHandler) {
    return new AutoValue_RunState(
        workflowInstance, state, time.get().toEpochMilli(), tries, INITIAL_RETRY_COST, 0L, lastExit,
        of(executionId), empty(), time, outputHandler);
  }
}
