/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.util.TimeUtil;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A specification of a scheduled workflow
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class WorkflowConfiguration {

  @JsonProperty
  public abstract String id();

  @JsonProperty
  public abstract Schedule schedule();

  @JsonProperty
  public abstract Optional<String> offset();

  @JsonProperty
  public abstract Optional<String> dockerImage();

  @JsonProperty
  public abstract Optional<List<String>> dockerArgs();

  /**
   * Toggles behavior to reliably report exit status from the Docker container, via
   * https://kubernetes.io/docs/tasks/debug-application-cluster/determine-reason-pod-failure/#writing-and-reading-a-termination-message
   *
   * <p>Ideally this should be unneeded, but mere exitCode is known to sometimes spuriously
   * return 0 when in fact the container has been killed. See https://github.com/kubernetes/kubernetes/issues/41516
   */
  @JsonProperty
  public abstract boolean dockerTerminationLogging();

  @JsonProperty
  public abstract Optional<Secret> secret();

  @JsonProperty
  public abstract Optional<String> serviceAccount();

  @JsonProperty
  public abstract List<String> resources();

  @JsonCreator
  public static WorkflowConfiguration create(
      @JsonProperty("id") String id,
      @JsonProperty("schedule") Schedule schedule,
      @JsonProperty("offset") Optional<String> offset,
      @JsonProperty("docker_image") Optional<String> dockerImage,
      @JsonProperty("docker_args") Optional<List<String>> dockerArgs,
      @JsonProperty("docker_termination_logging") Optional<Boolean> dockerTerminationLogging,
      @JsonProperty("secret") Optional<Secret> secret,
      @JsonProperty("service_account") Optional<String> serviceAccount,
      @JsonProperty("resources") List<String> resources) {

    return new AutoValue_WorkflowConfiguration(
        id, schedule, offset, dockerImage, dockerArgs,
        dockerTerminationLogging.orElse(false), secret, serviceAccount,
        resources == null ? Collections.emptyList() : resources);
  }

  public Instant addOffset(Instant next) {
    final String offset = offset().orElseGet(this::defaultOffset);

    return TimeUtil.addOffset(next.atZone(ZoneOffset.UTC), offset).toInstant();
  }

  private String defaultOffset() {
    switch (schedule().wellKnown()) {
      case HOURLY:
        return "PT1H";
      case DAILY:
        return "P1D";
      case WEEKLY:
        return "P1W";
      case MONTHLY:
        return "P1M";
      case YEARLY:
        return "P1Y";

      default:
        return "PT0S";
    }
  }

  @AutoValue
  @JsonIgnoreProperties(ignoreUnknown = true)
  public abstract static class Secret {

    @JsonProperty
    public abstract String name();

    @JsonProperty
    public abstract String mountPath();

    @JsonCreator
    public static Secret create(
        @JsonProperty("name") String name,
        @JsonProperty("mount_path") String mountPath) {
      return new AutoValue_WorkflowConfiguration_Secret(name, mountPath);
    }
  }

}
