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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import flyteidl.core.Execution;
import java.util.NoSuchElementException;
import org.junit.Test;


public class FlytePhaseTest {

  @Test
  public void testSuccessfulMapping() {
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.UNDEFINED), is(FlytePhase.UNDEFINED));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.QUEUED), is(FlytePhase.QUEUED));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.RUNNING), is(FlytePhase.RUNNING));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.SUCCEEDING), is(FlytePhase.SUCCEEDING));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.SUCCEEDED), is(FlytePhase.SUCCEEDED));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.FAILING), is(FlytePhase.FAILING));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.FAILED), is(FlytePhase.FAILED));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.ABORTED), is(FlytePhase.ABORTED));
    assertThat(FlytePhase.fromProto(Execution.WorkflowExecution.Phase.TIMED_OUT), is(FlytePhase.TIMED_OUT));
  }

  @Test
  public void testUnknownMapping() {
    assertThrows(NoSuchElementException.class, () -> FlytePhase.fromProto(Execution.WorkflowExecution.Phase.UNRECOGNIZED));
  }


}
