/*-
 * -\-\-
 * Spotify Styx CLI
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

package com.spotify.styx.cli;

import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.serialization.Json;
import java.util.List;
import java.util.Map.Entry;

/**
 * Cli output printer that prints json output
 */
class JsonCliOutput implements CliOutput {

  private static void printJson(Object object) {
    try {
      System.out.println(Json.serialize(object).utf8());
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    printJson(CliUtil.groupStates(runStateDataPayload.activeStates()).entrySet().stream()
        .collect(toMap(e -> e.getKey().toKey(), Entry::getValue)));
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    printJson(eventInfos);
  }

  @Override
  public void printBackfill(Backfill backfill, boolean ignored) {
    printJson(backfill);
  }

  @Override
  public void printBackfillPayload(BackfillPayload backfillPayload, boolean ignored) {
    printJson(backfillPayload);
  }

  @Override
  public void printBackfills(List<BackfillPayload> backfills, boolean ignored) {
    printJson(backfills);
  }

  @Override
  public void printResources(List<Resource> resources) {
    printJson(resources);
  }

  @Override
  public void printMessage(String message) {
    printJson(message);
  }

  @Override
  public void printWorkflow(Workflow workflow, WorkflowState state) {
    printJson(ImmutableMap.of("workflow", workflow, "state", state));
  }

  @Override
  public void printWorkflows(List<Workflow> workflows) {
    printJson(workflows);
  }

  @Override
  public void printError(String message) {
    System.err.println(message);
  }
}
