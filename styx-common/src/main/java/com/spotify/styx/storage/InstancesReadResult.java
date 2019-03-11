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

import static com.spotify.styx.storage.DatastoreStorage.activeWorkflowInstanceIndexShardName;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.immutables.builder.Builder;
import org.immutables.value.Value;


/**
 * A result of reading workflow instances from storage. Reads can be partially successful and the
 * {@link #instanceUnavailable(WorkflowInstance)} can be called to check whether an instance (or would have)
 * failed to be read from storage.
 */
public class InstancesReadResult {

  public enum Status {
    SUCCESS, // All instances were successfully read, no failures
    PARTIAL, // Some instances were successfully read, partial failure
    FAILURE  // No instances were successfully read, complete failure
  }

  private final Status status;
  private final Map<WorkflowInstance, RunState> instances;
  private final ReadInfo readInfo;

  @Builder.Constructor
  InstancesReadResult(
      Map<WorkflowInstance, RunState> instances,
      ReadInfo readInfo) {
    this.instances = Map.copyOf(instances);
    this.status = readInfo.status();
    this.readInfo = Objects.requireNonNull(readInfo);
  }

  /**
   * Get the read success level.
   */
  public Status status() {
    return status;
  }

  /**
   * Get all the workflow instances that were read. Might be a partial set in case of partial failures.
   */
  public Map<WorkflowInstance, RunState> instances() {
    return instances;
  }

  /**
   * Check whether a {@link WorkflowInstance} failed to be read (or would have if it existed). Note that this method
   * can return true also for instances that do not exist at all.
   */
  public boolean instanceUnavailable(WorkflowInstance instance) {
    return readInfo.instanceUnavailable(instance);
  }

  public static InstancesReadResult ofSuccess(Map<WorkflowInstance, RunState> instances) {
    return builder()
        .instances(instances)
        .readInfo(SuccessReadInfo.of())
        .build();
  }

  public static InstancesReadResultBuilder builder() {
    return new InstancesReadResultBuilder();
  }

  interface ReadInfo {
    Status status();
    boolean instanceUnavailable(WorkflowInstance instance);
  }

  interface SuccessReadInfo extends ReadInfo {

    @Override
    default Status status() {
      return Status.SUCCESS;
    }

    @Override
    default boolean instanceUnavailable(WorkflowInstance instance) {
      return false;
    }

    static SuccessReadInfo of() {
      return new SuccessReadInfo() {};
    }
  }

  @Value.Immutable
  public interface DatastoreReadInfo extends ReadInfo {

    int shardCount();
    Set<String> unavailableShards();
    int instanceCount();
    Set<WorkflowInstance> unavailableInstances();

    @Override
    default Status status() {
      if (unavailableShards().isEmpty() && unavailableInstances().isEmpty()) {
        return InstancesReadResult.Status.SUCCESS;
      } else if (unavailableShards().size() == shardCount() ||
                 unavailableInstances().size() == instanceCount()) {
        return InstancesReadResult.Status.FAILURE;
      } else {
        return InstancesReadResult.Status.PARTIAL;
      }
    }

    @Override
    default boolean instanceUnavailable(WorkflowInstance instance) {
      return
          // Did the workflow instance read fail?
          unavailableInstances().contains(instance) ||
          // Did we fail to read the shard of the workflow instance?
          unavailableShards().contains(activeWorkflowInstanceIndexShardName(instance.toKey()));
    }

    static ImmutableDatastoreReadInfo.Builder builder() {
      return ImmutableDatastoreReadInfo.builder();
    }
  }
}
