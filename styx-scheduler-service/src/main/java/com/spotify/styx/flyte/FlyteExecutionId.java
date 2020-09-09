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

import com.google.common.base.Preconditions;
import flyteidl.admin.ExecutionOuterClass;
import io.norberg.automatter.AutoMatter;

@AutoMatter
public interface FlyteExecutionId {

  String project();
  String domain();
  String name();

  static FlyteExecutionId create(final String project, final String domain, final String name) {
    return newBuilder()
        .project(project)
        .domain(domain)
        .name(name)
        .build();
  }

  static FlyteExecutionId fromProto(ExecutionOuterClass.ExecutionCreateResponse response) {
    return newBuilder()
        .project(response.getId().getProject())
        .domain(response.getId().getDomain())
        .name(response.getId().getName())
        .build();
  }

  static FlyteExecutionIdBuilder newBuilder() {
    return new FlyteExecutionIdBuilder();
  }
  /**
   * ex:<project>:<domain>:<name>.
   * @param urn
   * @return
   */
  static FlyteExecutionId fromUrn(String urn) {
    final String[] splitted = urn.split(":");
    Preconditions.checkArgument(splitted.length == 4, "Expected 4 parts in URN.");
    Preconditions.checkArgument(splitted[0].equals("ex"), "Expected URN to start with ex.");
    return newBuilder()
        .project(splitted[1])
        .domain(splitted[2])
        .name(splitted[3])
        .build();
  }

  default String toUrn() {
    return "ex" + ":" + project() + ":" + domain() + ":" + name();
  }

}
