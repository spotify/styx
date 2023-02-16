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

import static com.spotify.styx.state.RunState.State.QUEUED;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.spotify.styx.model.Event;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessageUtilTest {

  private static final List<String> DEPLETED_RESOURCES = List.of("foo", "bar"); // Unordered
  private static final Message MESSAGE = Message.info("Resource limit reached for: [bar, foo]");
  private static final Message STALE_MESSAGE = Message.info("Resource limit reached for: [baz]");
  private static final Event INFO = Event.info(WORKFLOW_INSTANCE, MESSAGE);
  private static final StateData DATA_WITH_MESSAGE = StateData.newBuilder()
      .messages(MESSAGE)
      .build();
  private static final StateData DATA_WITH_STALE_MESSAGE = StateData.newBuilder()
      .messages(STALE_MESSAGE)
      .build();
  private static final RunState RUNSTATE_WITH_MESSAGE = RunState.create(
      WORKFLOW_INSTANCE, QUEUED, DATA_WITH_MESSAGE, Instant.now(), 17);
  private static final RunState RUNSTATE_WITH_STALE_MESSAGE = RunState.create(
      WORKFLOW_INSTANCE, QUEUED, DATA_WITH_STALE_MESSAGE, Instant.now(), 17);

  @Mock private StateManager stateManager;

  @Before
  public void setUp() {
    assertThat(DEPLETED_RESOURCES.stream().sorted(), is(not(DEPLETED_RESOURCES)));
  }

  @Test
  public void shouldNotEmitResourceLimitReachedMessage() {
    MessageUtil.emitResourceLimitReachedMessage(stateManager, RUNSTATE_WITH_MESSAGE, DEPLETED_RESOURCES);
    verifyZeroInteractions(stateManager);
  }

  @Test
  public void shouldEmitResourceLimitReachedMessage() {
    MessageUtil.emitResourceLimitReachedMessage(stateManager, RUNSTATE_WITH_STALE_MESSAGE, DEPLETED_RESOURCES);
    verify(stateManager).receiveIgnoreClosed(INFO, RUNSTATE_WITH_MESSAGE.counter());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoResources() {
    MessageUtil.emitResourceLimitReachedMessage(stateManager, RUNSTATE_WITH_STALE_MESSAGE, Collections.emptyList());
  }
}
