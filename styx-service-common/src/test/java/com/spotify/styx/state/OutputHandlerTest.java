/*-
 * -\-\-
 * Spotify Styx Service Common
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

package com.spotify.styx.state;

import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OutputHandlerTest {

  @Mock private OutputHandler outputHandler1;
  @Mock private OutputHandler outputHandler2;
  @Mock private RunState runState;

  @Test
  public void fanOutput() {
    var outputHandler = OutputHandler.fanOutput(outputHandler1, outputHandler2);
    assertThat(outputHandler, is(instanceOf(FanOutputHandler.class)));
    outputHandler.transitionInto(runState);
    verify(outputHandler1).transitionInto(runState);
    verify(outputHandler2).transitionInto(runState);
  }

  @Test
  public void mdcDecorating() {
    var outputHandler = OutputHandler.mdcDecorating(outputHandler1);
    assertThat(outputHandler, is(instanceOf(MDCDecoratingHandler.class)));
    when(runState.workflowInstance()).thenReturn(WORKFLOW_INSTANCE);
    when(runState.state()).thenReturn(RunState.State.RUNNING);
    when(runState.counter()).thenReturn(17L);
    outputHandler.transitionInto(runState);
    verify(outputHandler1).transitionInto(runState);
  }
}