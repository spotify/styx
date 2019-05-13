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

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import java.util.List;

/**
 * Cli printing interface
 */
interface CliOutput {

  void printStates(RunStateDataPayload runStateDataPayload);

  void printEvents(List<EventInfo> eventInfos);

  void printBackfill(Backfill backfill, boolean noTruncate);

  void printBackfillPayload(BackfillPayload backfillPayload, boolean noTruncate);

  void printBackfills(List<BackfillPayload> backfills, boolean noTruncate);

  void printResources(List<Resource> resources);

  void printMessage(String message);

  void printWorkflow(Workflow workflow, WorkflowState state);

  void printWorkflows(List<Workflow> workflows);

  void printError(String message);
}
