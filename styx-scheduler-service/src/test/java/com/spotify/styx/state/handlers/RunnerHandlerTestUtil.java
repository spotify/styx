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

package com.spotify.styx.state.handlers;

import static com.spotify.styx.testdata.TestData.EXECUTION_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.util.IsClosedException;
import java.time.Instant;

class RunnerHandlerTestUtil {

  private static final Instant NOW = Instant.now();
  private static final long COUNTER = 17;

  private RunnerHandlerTestUtil() {
    // no instantiation
  }

  protected static void shouldHaltIfMissingExecutionId(RunState.State state,
                                                       ExecutionDescription executionDescription,
                                                       EventRouter eventRouter,
                                                       OutputHandler handler) {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, StateData.newBuilder()
        .executionDescription(executionDescription)
        .build(), NOW, COUNTER);

    handler.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), COUNTER);
    verifyNoMoreInteractions(eventRouter);
  }

  protected static void shouldHaltIfMissingExecutionDescription(RunState.State state,
                                                                EventRouter eventRouter,
                                                                OutputHandler handler) {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .build(), NOW, COUNTER);

    handler.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), COUNTER);
    verifyNoMoreInteractions(eventRouter);
  }

  protected static void shouldTransitionIntoSubmitted(ExecutionDescription executionDescription,
                                                      EventRouter eventRouter,
                                                      OutputHandler handler,
                                                      String runnerId)
      throws IsClosedException {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(executionDescription)
        .build());

    handler.transitionInto(runState, eventRouter);

    verify(eventRouter,  timeout(60_000)).receive(Event.submitted(WORKFLOW_INSTANCE, EXECUTION_ID, runnerId),
        runState.counter());
  }
}
