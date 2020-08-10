/*-
 * -\-\-
 * Spotify Styx Flyte Client
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

package com.spotify.styx.flyte.client;

import com.google.auto.value.AutoValue;
import flyteidl.core.IdentifierOuterClass;

@AutoValue
public abstract class LaunchPlanIdentifier {

  public abstract String domain();

  public abstract String project();

  public abstract String name();

  public abstract String version();

  public static LaunchPlanIdentifier create(
      String domain,
      String project,
      String name,
      String version) {
    return new AutoValue_LaunchPlanIdentifier(domain, project, name, version);
  }

  IdentifierOuterClass.Identifier toProto() {
    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
        .setDomain(domain())
        .setProject(project())
        .setName(name())
        .setVersion(version())
        .build();
  }
}
