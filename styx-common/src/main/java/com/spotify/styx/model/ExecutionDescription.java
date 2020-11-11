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

package com.spotify.styx.model;

import io.norberg.automatter.AutoMatter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoMatter
public interface ExecutionDescription {

  Optional<String> dockerImage();
  List<String> dockerArgs();
  boolean dockerTerminationLogging();
  Optional<WorkflowConfiguration.Secret> secret();
  Optional<String> serviceAccount();
  Optional<String> commitSha();
  Map<String, String> env();
  Optional<Duration> runningTimeout();
  Optional<String> retryCondition();
  Optional<FlyteExecConf> flyteExecConf();
  Optional<String> flyteExecutionId();

  static ExecutionDescriptionBuilder builder() {
    return new ExecutionDescriptionBuilder();
  }

  static ExecutionDescription forImage(String dockerImage) {
    return builder().dockerImage(dockerImage).build();
  }
}
