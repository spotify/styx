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
package com.spotify.styx.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import com.spotify.styx.model.WorkflowState;
import java.util.Optional;
import org.junit.Test;

public class WorkflowStateUtilTest {

  private static final String ORIGINAL_COMMIT_SHA = "3caec76e5703ad6181d211d2461e648d2166b1c0";
  private static final String PATCHED_COMMIT_SHA = "0000c76e5703ad6181d211d2461e648d2166b1c0";

  private WorkflowState FULLY_POPULATED_STATE = WorkflowState.all(true, "original_docker_image", ORIGINAL_COMMIT_SHA);

  @Test
  public void patchAnEmptyStateReturnsPatch() {
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(Optional.empty(), FULLY_POPULATED_STATE);
    assertThat(patchedState, equalTo(FULLY_POPULATED_STATE));
  }

  @Test
  public void patchStateWithAnEmptyPatchReturnsOriginal() {
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        WorkflowState.empty());
    assertThat(patchedState, equalTo(FULLY_POPULATED_STATE));
  }

  @Test
  public void patchEnabledFieldReturnsPatchedOriginal() {
    WorkflowState patch = WorkflowState.create(Optional.of(false), Optional.empty(), Optional.empty());
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        patch);
    assertThat(patchedState, equalTo(WorkflowState.all(false, "original_docker_image", ORIGINAL_COMMIT_SHA)));
  }

  @Test
  public void nonPopulatedNorPatchedEnabledShouldBeFalseAfterPatch() {
    WorkflowState patch = WorkflowState.empty();
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(
        Optional.of(
            WorkflowState.create(
                Optional.empty(),
                Optional.of("original_docker_image"),
                Optional.of(ORIGINAL_COMMIT_SHA))),
        patch);
    assertThat(patchedState, equalTo(WorkflowState.all(false, "original_docker_image", ORIGINAL_COMMIT_SHA)));
  }

  @Test
  public void patchDockerImageFieldReturnsPatchedOriginal() {
    WorkflowState patch = WorkflowState.create(Optional.empty(), Optional.of("patched_docker_image"), Optional.empty());
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        patch);
    assertThat(patchedState, equalTo(WorkflowState.all(true, "patched_docker_image", ORIGINAL_COMMIT_SHA)));
  }

  @Test
  public void patchCommitShaFieldReturnsPatchedOriginal() {
    WorkflowState patch = WorkflowState.create(Optional.empty(), Optional.empty(), Optional.of(PATCHED_COMMIT_SHA));
    WorkflowState patchedState = WorkflowStateUtil.patchWorkflowState(
        Optional.of(FULLY_POPULATED_STATE),
        patch);
    assertThat(patchedState, equalTo(WorkflowState.all(true, "original_docker_image", PATCHED_COMMIT_SHA)));
  }

}
