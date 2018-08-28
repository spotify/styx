/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx;

import static java.util.stream.Collectors.toList;

import com.spotify.styx.model.Event;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import java.util.List;

public class MessageUtil {

  private MessageUtil() {
    throw new UnsupportedOperationException();
  }

  public static void emitResourceLimitReachedMessage(StateManager stateManager, RunState runState,
      List<String> depletedResources) {
    if (depletedResources.isEmpty()) {
      throw new IllegalArgumentException();
    }
    final List<String> depletedResourcesOrdered = depletedResources.stream().sorted().collect(toList());
    final Message message = Message.info("Resource limit reached for: " + depletedResourcesOrdered);
    if (!runState.data().message().map(message::equals).orElse(false)) {
      stateManager.receiveIgnoreClosed(Event.info(runState.workflowInstance(), message), runState.counter());
    }
  }
}
