/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.testdata.TestData;
import java.net.URI;
import java.util.List;
import org.junit.Test;

public class TickerManagerTest {

  private static final URI WURI = URI.create("http://foo");
  private static final Workflow HOURLY_WORKFLOW =
      Workflow.create("id1", WURI, TestData.HOURLY_DATA_ENDPOINT);
  private static final Workflow DAILY_WORKFLOW =
      Workflow.create("id1", WURI, TestData.DAILY_DATA_ENDPOINT);

  private final TickerManager manager = new TickerManager(this::create);
  private final List<TickTock> tickTocks = Lists.newArrayList();

  private TickTock create(Workflow workflow) {
    TickTock tickTock = mock(TickTock.class);
    when(tickTock.workflow()).thenReturn(workflow);
    tickTocks.add(tickTock);
    return tickTock;
  }

  @Test
  public void shouldStartTickTock() throws Exception {
    manager.updateWorkflow(HOURLY_WORKFLOW);

    verify(tickTocks.get(0), times(1)).start();
    verify(tickTocks.get(0), never()).close();
  }

  @Test
  public void shouldOnlyStartTickTockOnce() throws Exception {
    manager.updateWorkflow(HOURLY_WORKFLOW);
    manager.updateWorkflow(HOURLY_WORKFLOW);

    verify(tickTocks.get(0), times(1)).start();
    verify(tickTocks.get(0), never()).close();
  }

  @Test
  public void shouldClosePreviousTickTockOnChange() throws Exception {
    manager.updateWorkflow(HOURLY_WORKFLOW);
    manager.updateWorkflow(DAILY_WORKFLOW);

    verify(tickTocks.get(0), times(1)).start();
    verify(tickTocks.get(0), times(1)).close();
    verify(tickTocks.get(1), times(1)).start();
    verify(tickTocks.get(1), never()).close();
  }

  @Test
  public void shouldClosePreviousTickTockOnRemoved() throws Exception {
    manager.updateWorkflow(HOURLY_WORKFLOW);
    manager.removeWorkflow(HOURLY_WORKFLOW);

    assertThat(tickTocks.size(), is(1));
    verify(tickTocks.get(0), times(1)).start();
    verify(tickTocks.get(0), times(1)).close();
  }
}
