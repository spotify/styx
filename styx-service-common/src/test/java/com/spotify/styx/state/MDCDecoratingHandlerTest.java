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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.spotify.styx.state.RunState.State;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.MDC;

@RunWith(JUnitParamsRunner.class)
public class MDCDecoratingHandlerTest {

  private static final long COUNTER = 17L;

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private OutputHandler delegate;
  @Mock private RunState runState;
  @Mock private EventRouter eventRouter;

  private MDCDecoratingHandler sut;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(runState.workflowInstance()).thenReturn(WORKFLOW_INSTANCE);
    sut = new MDCDecoratingHandler(delegate);
  }

  @Parameters(source = State.class)
  @Test
  public void shouldDecorateMDC(State state) {
    when(runState.state()).thenReturn(state);
    when(runState.counter()).thenReturn(COUNTER);

    var id = new AtomicReference<String>();
    var stateCounter = new AtomicReference<String>();
    var stateName = new AtomicReference<String>();

    doAnswer(a -> {
      id.set(MDC.get("wfi-id"));
      stateCounter.set(MDC.get("wfi-state-counter"));
      stateName.set(MDC.get("wfi-state-name"));
      return null;
    }).when(delegate).transitionInto(any(), any());

    sut.transitionInto(runState, eventRouter);

    assertThat(id.get(), is(WORKFLOW_INSTANCE.toString()));
    assertThat(stateCounter.get(), is(String.valueOf(COUNTER)));
    assertThat(stateName.get(), is(state.toString()));
  }

  @Test
  public void propagatesIOException() {
    when(runState.state()).thenReturn(State.RUNNING);
    when(runState.counter()).thenReturn(COUNTER);

    var cause = new IOException("foo!");

    doAnswer(a -> sneakyThrow(cause)).when(delegate).transitionInto(any(), any());

    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.transitionInto(runState, eventRouter);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> Void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }
}
