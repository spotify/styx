/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.storage;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.InstancesReadResult.Status;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class InstancesReadResultTest {

  private static final Instant NOW = Instant.now();
  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("foo", "bar");
  private static final WorkflowInstance WORKFLOW_INSTANCE_1 = WorkflowInstance.create(WORKFLOW_ID, "1");
  private static final WorkflowInstance WORKFLOW_INSTANCE_2 = WorkflowInstance.create(WORKFLOW_ID, "2");

  private static final RunState RUN_STATE_1 = RunState.create(WORKFLOW_INSTANCE_1, RunState.State.RUNNING, NOW);
  private static final RunState RUN_STATE_2 = RunState.create(WORKFLOW_INSTANCE_2, RunState.State.RUNNING, NOW);

  @Test
  public void shouldIndicateSuccess() {
    assertThat(InstancesReadResult.ofSuccess(Map.of()).status(), is(Status.SUCCESS));
  }

  @Test
  public void shouldIndicateSuccessWhenAllShardsAreAvailableButNoInstancesRead() {
    var result = InstancesReadResult.builder()
        .instances(Map.of())
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(128)
            .unavailableShards(Set.of())
            .instanceCount(0)
            .unavailableInstances(Set.of())
            .build())
        .build();
    assertThat(result.status(), is(Status.SUCCESS));
  }

  @Test
  public void shouldIndicateSuccessWhenAllShardsAreAvailableAndAllInstancesAvailable() {
    var result = InstancesReadResult.builder()
        .instances(Map.of(WORKFLOW_INSTANCE_1, RUN_STATE_1, WORKFLOW_INSTANCE_2, RUN_STATE_2))
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(128)
            .unavailableShards(Set.of())
            .instanceCount(2)
            .unavailableInstances(Set.of())
            .build())
        .build();
    assertThat(result.status(), is(Status.SUCCESS));
  }

  @Test
  public void shouldIndicatePartialFailureWhenShardsAreUnavailable() {
    var result = InstancesReadResult.builder()
        .instances(Map.of(WORKFLOW_INSTANCE_1, RUN_STATE_1, WORKFLOW_INSTANCE_2, RUN_STATE_2))
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(128)
            .unavailableShards(Set.of("shard-1", "shard-85"))
            .instanceCount(2)
            .unavailableInstances(Set.of())
            .build())
        .build();
    assertThat(result.status(), is(Status.PARTIAL));
  }

  @Test
  public void shouldIndicateCompleteFailureWhenAllShardsAreUnavailable() {
    var result = InstancesReadResult.builder()
        .instances(Map.of())
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(2)
            .addUnavailableShards("shard-1", "shard-85")
            .instanceCount(0)
            .unavailableInstances(Set.of())
            .build())
        .build();
    assertThat(result.status(), is(Status.FAILURE));
  }

  @Test
  public void shouldIndicatePartialFailureWhenInstancesAreUnavailable() {
    var result = InstancesReadResult.builder()
        .instances(Map.of(WORKFLOW_INSTANCE_1, RUN_STATE_1))
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(128)
            .unavailableShards(Set.of())
            .instanceCount(2)
            .unavailableInstances(Set.of(WORKFLOW_INSTANCE_2))
            .build())
        .build();
    assertThat(result.status(), is(Status.PARTIAL));
  }

  @Test
  public void shouldIndicateCompleteFailureWhenAllInstancesAreUnavailable() {
    var result = InstancesReadResult.builder()
        .instances(Map.of())
        .readInfo(InstancesReadResult.DatastoreReadInfo.builder()
            .shardCount(128)
            .unavailableShards(Set.of())
            .instanceCount(2)
            .unavailableInstances(Set.of(WORKFLOW_INSTANCE_1, WORKFLOW_INSTANCE_2))
            .build())
        .build();
    assertThat(result.status(), is(Status.FAILURE));
  }
}
