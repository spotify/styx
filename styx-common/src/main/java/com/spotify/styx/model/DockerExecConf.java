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

import io.norberg.automatter.AutoMatter;
import java.util.List;
import java.util.Optional;

@AutoMatter
public interface DockerExecConf {
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
}
