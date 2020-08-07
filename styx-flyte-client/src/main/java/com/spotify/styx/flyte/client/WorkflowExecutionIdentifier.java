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
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;

@AutoValue
public abstract class WorkflowExecutionIdentifier {

  public abstract String domain();

  public abstract String project();

  public abstract String name();

  static WorkflowExecutionIdentifier fromProto(final ExecutionOuterClass.ExecutionCreateResponse response) {
    final IdentifierOuterClass.WorkflowExecutionIdentifier id = response.getId();
    return create(id.getDomain(), id.getProject(), id.getName());
  }

  static WorkflowExecutionIdentifier create(final String domain, final String project, final String name) {
    return new AutoValue_WorkflowExecutionIdentifier(domain, project, name);
  }
}
