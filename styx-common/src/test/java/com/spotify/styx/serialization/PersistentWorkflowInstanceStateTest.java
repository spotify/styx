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

package com.spotify.styx.serialization;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

import com.spotify.styx.state.RunState;
import com.spotify.styx.testdata.TestData;
import org.junit.Test;

public class PersistentWorkflowInstanceStateTest {

  @Test
  public void testOfCounter() throws Exception {
    final long counter = 17L;
    final PersistentWorkflowInstanceState persistentState = PersistentWorkflowInstanceState.of(counter);
    assertThat(persistentState.state(), is(nullValue()));
    assertThat(persistentState.data(), is(nullValue()));
    assertThat(persistentState.timestamp(), is(nullValue()));
    assertThat(persistentState.counter(), is(counter));
  }

  @Test
  public void testOfRunStateAndCounter() throws Exception {
    final RunState runState = RunState.fresh(TestData.WORKFLOW_INSTANCE);
    final long counter = 17L;
    final PersistentWorkflowInstanceState persistentState = PersistentWorkflowInstanceState.of(runState, counter);
    assertThat(persistentState.state(), is(runState.state()));
    assertThat(persistentState.data(), is(runState.data()));
    assertThat(persistentState.timestamp().toEpochMilli(), is(runState.timestamp()));
    assertThat(persistentState.counter(), is(counter));
  }
}