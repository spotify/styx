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

import static com.spotify.styx.flyte.FlyteAdminClientRunnerUtil.getExecutionMode;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.RELAUNCH;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.UNRECOGNIZED;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateDataBuilder;
import com.spotify.styx.state.Trigger;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class FlyteAdminClientRunnerUtilTest {

  private static StateData NATURAL = StateData.newBuilder().trigger(Trigger.natural()).build();
  private static StateData BACKFILL = StateData.newBuilder().trigger(Trigger.backfill("id")).build();
  private static StateData ADHOC = StateData.newBuilder().trigger(Trigger.adhoc("id")).build();
  private static StateData UNKNOWN = StateData.newBuilder().trigger(Trigger.unknown("id")).build();
  private static StateData MISSING = StateData.newBuilder().build();

  @Test
  public void testGetExecutionMode() {
    assertThat(getExecutionMode(NATURAL), is(SCHEDULED));
    assertThat(getExecutionMode(BACKFILL), is(RELAUNCH));
    assertThat(getExecutionMode(ADHOC), is(MANUAL));
    assertThat(getExecutionMode(UNKNOWN), is(UNRECOGNIZED));
    assertThat(getExecutionMode(MISSING), is(UNRECOGNIZED));
  }

  @Test
  @Parameters(method = "stateData")
  public void testGetExecutionModeHasRetried(StateData data) {
    final StateData hasRetried = StateDataBuilder.from(data).tries(1).build();
    assertThat(getExecutionMode(hasRetried), is(RELAUNCH));
  }

  @SuppressWarnings("unused")
  private static StateData[] stateData() {
    return new StateData[] {
        NATURAL,
        MISSING,
        ADHOC,
        BACKFILL,
        UNKNOWN
    };
  }
}
