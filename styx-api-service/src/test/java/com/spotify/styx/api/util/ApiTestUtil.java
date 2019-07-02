/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.api.util;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.WorkflowId;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class ApiTestUtil {

  private ApiTestUtil() {
    // no instantiation
  }

  public static SortedMap<WorkflowId, SortedSet<RunStateDataPayload.RunStateData>> groupStatesByWorkflow(
      List<RunStateDataPayload.RunStateData> runStateDataList) {
    return runStateDataList.stream()
        .collect(groupingBy(
            state -> state.workflowInstance().workflowId(),
            ApiTestUtil::newSortedWorkflowIdSet,
            toCollection(ApiTestUtil::newSortedStateSet)
        ));
  }

  private static TreeSet<RunStateDataPayload.RunStateData> newSortedStateSet() {
    return Sets.newTreeSet(RunStateDataPayload.RunStateData.WORKFLOW_COMPARATOR);
  }

  private static TreeMap<WorkflowId, SortedSet<RunStateDataPayload.RunStateData>> newSortedWorkflowIdSet() {
    return Maps.newTreeMap(WorkflowId.KEY_COMPARATOR);
  }
}
