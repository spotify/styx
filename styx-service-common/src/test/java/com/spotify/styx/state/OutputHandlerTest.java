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