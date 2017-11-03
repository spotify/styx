/*-
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

package com.spotify.styx.util;

import static com.spotify.styx.util.WorkflowStateUtil.patchWorkflowState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import com.spotify.styx.model.WorkflowState;
import java.time.Instant;
import java.util.Optional;
import org.junit.Test;

public class WorkflowStateUtilTest {

  private WorkflowState FULLY_POPULATED_STATE = WorkflowState.builder()
      .enabled(true)
      .nextNaturalTrigger(Instant.parse("2017-11-03T01:23:00Z"))
      .nextNaturalOffsetTrigger(Instant.parse("2017-11-03T02:23:00Z"))
      .build();

  @Test
  public void patchAnEmptyStateReturnsPatch() {
    WorkflowState patchedState = patchWorkflowState(Optional.empty(), FULLY_POPULATED_STATE);
    assertThat(patchedState, equalTo(FULLY_POPULATED_STATE));
  }

  @Test
  public void patchStateWithAnEmptyPatchReturnsOriginal() {
    WorkflowState patchedState = patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        WorkflowState.empty());
    assertThat(patchedState, equalTo(FULLY_POPULATED_STATE));
  }

  @Test
  public void patchEnabledFieldReturnsPatchedOriginal() {
    WorkflowState patch = WorkflowState.patchEnabled(false);
    WorkflowState patchedState = patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        patch);
    assertThat(patchedState.enabled(), equalTo(patch.enabled()));
    assertThat(patchedState.nextNaturalTrigger().toString(),
               equalTo(FULLY_POPULATED_STATE.nextNaturalTrigger().toString()));
    assertThat(patchedState.nextNaturalOffsetTrigger().toString(),
               equalTo(FULLY_POPULATED_STATE.nextNaturalOffsetTrigger().toString()));
  }

  @Test
  public void nonPopulatedNorPatchedEnabledShouldBeFalseAfterPatch() {
    WorkflowState patch = WorkflowState.empty();
    WorkflowState patchedState = patchWorkflowState(
        Optional.of(WorkflowState.patchEnabled(false)),
        patch);
    assertThat(patchedState, equalTo(WorkflowState.patchEnabled(false)));
  }
}
