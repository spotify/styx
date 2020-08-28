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

public class FlyteExecution {

  private final String project;
  private final String domain;
  private final String name;

  FlyteExecution(final String project, final String domain, final String name) {
    this.project = project;
    this.domain = domain;
    this.name = name;
  }

  public static FlyteExecution fromProto(ExecutionOuterClass.ExecutionCreateResponse response) {
    return new FlyteExecution(
        response.getId().getProject(),
        response.getId().getDomain(),
        response.getId().getName());
  }

  /**
   * ex:<project>:<domain>:<name>.
   * @param urn
   * @return
   */
  public static FlyteExecution fromUrn(String urn) {
    final String[] splitted = urn.split(":");
    Preconditions.checkArgument(splitted.length == 4);
    Preconditions.checkArgument(splitted[0].equals("ex"));
    return new FlyteExecution(splitted[1], splitted[2], splitted[3]);
  }

  public String toUrn() {
    return "ex" + ":" + project + ":" + domain + ":" + name;
  }

  public String getProject() {
    return project;
  }

  public String getDomain() {
    return domain;
  }

  public String getName() {
    return name;
  }
}
