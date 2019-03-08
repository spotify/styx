/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.model;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventVisitorAdapterTest {

  @Mock WorkflowInstance workflowInstance;
  @Mock Trigger trigger;
  @Mock TriggerParameters parameters;
  @Mock Message message;
  @Mock ExecutionDescription executionDescription;

  private final EventVisitorAdapter<Void> sut = new EventVisitorAdapter<>();

  @Test
  public void triggerExecution() {
    assertThat(sut.triggerExecution(workflowInstance, trigger, parameters), is(nullValue()));
  }

  @Test
  public void info() {
    assertThat(sut.info(workflowInstance, message), is(nullValue()));
  }

  @Test
  public void dequeue() {
    assertThat(sut.dequeue(workflowInstance, Set.of()), is(nullValue()));
  }

  @Test
  public void submit() {
    assertThat(sut.submit(workflowInstance, executionDescription, ""), is(nullValue()));
  }

  @Test
  public void submitted() {
    assertThat(sut.submitted(workflowInstance, ""), is(nullValue()));
  }

  @Test
  public void started() {
    assertThat(sut.started(workflowInstance), is(nullValue()));
  }

  @Test
  public void terminate() {
    assertThat(sut.terminate(workflowInstance, Optional.empty()), is(nullValue()));
  }

  @Test
  public void runError() {
    assertThat(sut.runError(workflowInstance, ""), is(nullValue()));
  }

  @Test
  public void success() {
    assertThat(sut.success(workflowInstance), is(nullValue()));
  }

  @Test
  public void retryAfter() {
    assertThat(sut.retryAfter(workflowInstance, 0), is(nullValue()));
  }

  @Test
  public void stop() {
    assertThat(sut.stop(workflowInstance), is(nullValue()));
  }

  @Test
  public void timeout() {
    assertThat(sut.timeout(workflowInstance), is(nullValue()));
  }

  @Test
  public void halt() {
    assertThat(sut.halt(workflowInstance), is(nullValue()));
  }

  @Test
  public void timeTrigger() {
    assertThat(sut.timeTrigger(workflowInstance), is(nullValue()));
  }

  @Test
  public void created() {
    assertThat(sut.created(workflowInstance, "", ""), is(nullValue()));
  }

  @Test
  public void retry() {
    assertThat(sut.retry(workflowInstance), is(nullValue()));
  }
}
