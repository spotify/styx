/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.testdata.TestData;
import java.util.List;
import org.junit.Test;

public class FanOutputHandlerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  @Test(expected = FooException.class)
  public void shouldThrowIfOutputHandlerFails() {
    OutputHandler failingOutputHandler = state -> {
      throw new FooException();
    };
    FanOutputHandler fanOutputHandler = new FanOutputHandler(List.of(failingOutputHandler));
    fanOutputHandler.transitionInto(RunState.fresh(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldSkipAtMostOnceOutputHandlers() {
    AtMostOnceOutputHandler atMostOnceOutputHandler = mock(AtMostOnceOutputHandler.class);
    OutputHandler outputHandler = mock(OutputHandler.class);
    FanOutputHandler fanOutputHandler = new FanOutputHandler(List.of(atMostOnceOutputHandler, outputHandler));
    final RunState state = RunState.fresh(WORKFLOW_INSTANCE);
    fanOutputHandler.tryTransitionInto(state);
    verify(outputHandler).transitionInto(state);
    verifyZeroInteractions(atMostOnceOutputHandler);
  }

  private class FooException extends RuntimeException {
  }
}
