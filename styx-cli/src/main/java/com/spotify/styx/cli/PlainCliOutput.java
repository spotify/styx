/*
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

import com.spotify.apollo.Response;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.api.cli.EventsPayload.TimestampedPersistentEvent;
import com.spotify.styx.model.EventSerializer;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.util.EventUtil;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.SortedSet;

import okio.ByteString;

import static com.spotify.styx.cli.CliUtil.formatTimestamp;

/**
 * Cli output printer that prints more unix tool friendly output
 */
public class PlainCliOutput implements CliOutput {

  private static final Logger LOG = LoggerFactory.getLogger(PlainCliOutput.class);

  @Override
  public void parsed(Namespace namespace) {
    // noop
  }

  @Override
  public void parseError(ArgumentParserException e, String help) {
    LOG.warn(e.getMessage());
    LOG.info(help);
  }

  @Override
  public void apiError(Throwable throwable) {
    LOG.warn("An API error occurred", throwable);
  }

  @Override
  public void header(Main.Command command) {
    // noop
  }

  @Override
  public void printActiveStates(ActiveStatesPayload activeStatesPayload) {
    LOG.info("COMPONENT WORKFLOW INSTANCE STATE LAST_EXECUTION_ID PREVIOUS_EXECUTION_INFO");

    SortedMap<WorkflowId, SortedSet<ActiveStatesPayload.ActiveState>> groupedActiveStates =
        CliUtil.groupActiveStates(activeStatesPayload.activeStates());

    for (WorkflowId workflowId : groupedActiveStates.keySet()) {
      for (ActiveStatesPayload.ActiveState activeState : groupedActiveStates.get(workflowId)) {
        final String previousExecutionInfo;
        if (activeState.previousExecutionLastEvent().isPresent()) {
          final EventSerializer.PersistentEvent
              persistentEvent = activeState.previousExecutionLastEvent().get();
          previousExecutionInfo = CliUtil.lastExecutionMessage(persistentEvent.toEvent());
        } else {
          previousExecutionInfo = "No data found";
        }
        LOG.info(String.format("%s %s %s %s %s %s",
                               workflowId.componentId(),
                               workflowId.endpointId(),
                               activeState.workflowInstance().parameter(),
                               activeState.state(),
                               activeState.lastExecutionId(),
                               previousExecutionInfo));
      }
    }
  }

  @Override
  public void printEvents(EventsPayload eventsPayload) {
    LOG.info("TIME EVENT DATA");

    for (TimestampedPersistentEvent timestampedEvent : eventsPayload.events()) {
      LOG.info(String.format("%s %s %s",
                             formatTimestamp(timestampedEvent.timestamp()),
                             EventUtil.name(timestampedEvent.event().toEvent()),
                             CliUtil.data(timestampedEvent.event().toEvent())));
    }
  }

  @Override
  public void printResponse(Response<ByteString> response) {
    LOG.info(response.status().toString());
  }
}
