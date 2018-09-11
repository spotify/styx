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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static org.fusesource.jansi.Ansi.ansi;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.WorkflowId;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.fusesource.jansi.Ansi;

class CliUtil {

  private CliUtil() {
    // no instantiation
  }

  static Ansi colored(Ansi.Color color, Object obj) {
    return ansi().fg(color).a(obj).reset();
  }

  static Ansi coloredBright(Ansi.Color color, Object obj) {
    return ansi().fgBright(color).a(obj).reset();
  }

  static SortedMap<WorkflowId, SortedSet<RunStateData>> groupStates(List<RunStateData> runStateDataList) {
    return runStateDataList.stream()
        .collect(groupingBy(
            state -> state.workflowInstance().workflowId(),
            CliUtil::newSortedWorkflowIdSet,
            toCollection(CliUtil::newSortedStateSet)
        ));
  }

  private static TreeSet<RunStateData> newSortedStateSet() {
    return Sets.newTreeSet(RunStateData.PARAMETER_COMPARATOR);
  }

  private static TreeMap<WorkflowId, SortedSet<RunStateData>> newSortedWorkflowIdSet() {
    return Maps.newTreeMap(WorkflowId.KEY_COMPARATOR);
  }

  static String formatTimestamp(long timestamp) {
    return Instant
        .ofEpochMilli(timestamp)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
  }

  static String formatMap(Map<String, String> map) {
    return Joiner.on(" ").withKeyValueSeparator("=")
        .join(ImmutableSortedMap.copyOf(map));
  }
}
