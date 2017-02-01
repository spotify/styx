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

import static com.spotify.styx.cli.CliUtil.formatTimestamp;

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.StateData;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Cli output printer that prints more unix tool friendly output
 */
class PlainCliOutput implements CliOutput {

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    SortedMap<WorkflowId, SortedSet<RunStateDataPayload.RunStateData>> groupedStates =
        CliUtil.groupStates(runStateDataPayload.activeStates());

    groupedStates.entrySet().forEach(entry -> {
      WorkflowId workflowId = entry.getKey();
      entry.getValue().forEach(RunStateData -> {
        final StateData stateData = RunStateData.stateData();
        final List<Message> messages = stateData.messages();

        final String lastMessage;
        if (messages.isEmpty()) {
          lastMessage = "No info";
        } else {
          final Message message = messages.get(messages.size() - 1);
          lastMessage = message.line();
        }

        System.out.println(String.format(
            "%s %s %s %s %s %d %s",
            workflowId.componentId(),
            workflowId.endpointId(),
            RunStateData.workflowInstance().parameter(),
            RunStateData.state(),
            stateData.executionId().orElse("<no-execution-id>"),
            stateData.tries(),
            lastMessage
        ));
      });
    });
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    eventInfos.forEach(
        eventInfo ->
            System.out.println(String.format("%s %s %s",
                                             formatTimestamp(eventInfo.timestamp()),
                                             eventInfo.name(),
                                             eventInfo.info()))
    );
  }

  @Override
  public void printBackfill(Backfill backfill) {
    System.out.println(String.format("Backfill id: %s", backfill.id()));
    System.out.println(String.format("Component id: %s", backfill.workflowId().componentId()));
    System.out.println(String.format("Workflow id: %s", backfill.workflowId().endpointId()));
    System.out.println(String.format("Start date/datehour (inclusive): %s", backfill.start()));
    System.out.println(String.format("End date/datehour (exclusive): %s", backfill.end()));
    if (!backfill.halted() && !backfill.completed()) {
      System.out.println(String.format("Next date/datehour: %s", backfill.nextTrigger()));
    }
    System.out.println(String.format("Completed: %s", backfill.completed()));
    System.out.println(String.format("Halted: %s", backfill.halted()));
  }

  @Override
  public void printBackfill(BackfillPayload backfillPayload) {
    printBackfill(backfillPayload.backfill());
    if (backfillPayload.statuses().isPresent()) {
      printStates(backfillPayload.statuses().get());
    }
  }
}
