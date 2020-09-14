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

import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.RELAUNCH;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.UNRECOGNIZED;

import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.TriggerUtil;
import flyteidl.admin.ExecutionOuterClass;

public class FlyteAdminClientRunnerUtil {

  private FlyteAdminClientRunnerUtil(){
    // no instantiation
  }

  public static ExecutionOuterClass.ExecutionMetadata.ExecutionMode getExecutionMode(StateData data) {
    if (data.tries() >= 1) {
      return RELAUNCH;
    }
    if (data.trigger().isEmpty()) {
      return UNRECOGNIZED;
    }
    final Trigger trigger = data.trigger().get();
    if (TriggerUtil.isBackfill(trigger)) {
      return RELAUNCH;
    }
    if (TriggerUtil.isNatural(trigger)) {
      return SCHEDULED;
    }
    if (TriggerUtil.isAdhoc(trigger)) {
      return MANUAL;
    }
    return UNRECOGNIZED;
  }
}
