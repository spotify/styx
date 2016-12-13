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

import com.google.auto.value.AutoValue;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import java.util.List;

/**
 * Cli printing interface
 */
interface CliOutput {

  void printActiveStates(ActiveStatesPayload activeStatesPayload);

  void printEvents(List<EventInfo> eventInfos);

  @AutoValue
  abstract class EventInfo {
    abstract long timestamp();
    abstract String name();
    abstract String info();

    public static EventInfo create(long ts, String eventName, String eventInfo) {
      return new AutoValue_CliOutput_EventInfo(ts, eventName, eventInfo);
    }
  }
}
