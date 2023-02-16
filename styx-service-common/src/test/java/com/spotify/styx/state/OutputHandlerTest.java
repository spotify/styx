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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OutputHandlerTest {

  @Mock private OutputHandler outputHandler1;
  @Mock private OutputHandler outputHandler2;
  @Mock private RunState runState;
  @Mock private EventRouter eventRouter;

  @Test
  public void fanOutput() {
    var outputHandler = OutputHandler.fanOutput(List.of(outputHandler1, outputHandler2));
    outputHandler.transitionInto(runState, eventRouter);
    verify(outputHandler1).transitionInto(runState, eventRouter);
    verify(outputHandler2).transitionInto(runState, eventRouter);
  }

  @Test
  public void mdcDecorating() {
    var outputHandler = OutputHandler.mdcDecorating(outputHandler1);
    assertThat(outputHandler, is(instanceOf(MDCDecoratingHandler.class)));
    when(runState.workflowInstance()).thenReturn(WORKFLOW_INSTANCE);
    when(runState.state()).thenReturn(RunState.State.RUNNING);
    when(runState.counter()).thenReturn(17L);
    outputHandler.transitionInto(runState, eventRouter);
    verify(outputHandler1).transitionInto(runState, eventRouter);
  }

  @Test
  public void tracing() {
    var outputHandlers = OutputHandler.tracing(List.of(outputHandler1, outputHandler2));
    assertThat(outputHandlers.size(), is(2));
    assertThat(Proxy.isProxyClass(outputHandlers.get(0).getClass()), is(true));
    assertThat(Proxy.isProxyClass(outputHandlers.get(1).getClass()), is(true));
    outputHandlers.get(0).transitionInto(runState, eventRouter);
    verify(outputHandler1).transitionInto(runState, eventRouter);
    outputHandlers.get(1).transitionInto(runState, eventRouter);
    verify(outputHandler2).transitionInto(runState, eventRouter);
  }
}
