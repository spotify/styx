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

import com.spotify.styx.model.Schedule.WellKnown;
import com.spotify.styx.util.TimeUtil;
import io.norberg.automatter.AutoMatter;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A specification of a scheduled workflow
 */
@AutoMatter
public interface WorkflowConfiguration {

  String id();

  Schedule schedule();

  Optional<String> offset();

  Optional<String> commitSha();

  Optional<String> dockerImage();

  Optional<List<String>> dockerArgs();

  /**
   * Toggles behavior to reliably report exit status from the Docker container, via
   * https://kubernetes.io/docs/tasks/debug-application-cluster/determine-reason-pod-failure/#writing-and-reading-a-termination-message
   *
   * <p>Ideally this should be unneeded, but mere exitCode is known to sometimes spuriously
   * return 0 when in fact the container has been killed. See https://github.com/kubernetes/kubernetes/issues/41516
   */
  boolean dockerTerminationLogging();

  Optional<String> serviceAccount();

  List<String> resources();

  Map<String, String> env();

  Optional<Duration> runningTimeout();

  Optional<String> retryCondition();

  Optional<FlyteExecConf> flyteExecConf();

  Optional<DeploymentSource> source();

  default Instant addOffset(Instant next) {
    final String offset = offset().orElseGet(this::defaultOffset);

    return TimeUtil.addOffset(next.atZone(ZoneOffset.UTC), offset).toInstant();
  }

  default Instant subtractOffset(Instant next) {
    final String offset = offset().orElseGet(this::defaultOffset);

    return TimeUtil.subtractOffset(next.atZone(ZoneOffset.UTC), offset).toInstant();
  }

  default String defaultOffset() {
    return defaultOffset(schedule());
  }

  default String defaultOffset(Schedule schedule) {
    return defaultOffset(schedule.wellKnown());
  }

  default String defaultOffset(WellKnown schedule) {
    switch (schedule) {
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

  static WorkflowConfigurationBuilder builder() {
    return new WorkflowConfigurationBuilder();
  }
}
