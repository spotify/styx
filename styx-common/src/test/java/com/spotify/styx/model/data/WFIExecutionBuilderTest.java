/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.model.data;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.spotify.styx.WorkflowInstanceEventFactory;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class WFIExecutionBuilderTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.parseKey("component1#endpoint1#2016-08-03T06");
  private static final WorkflowInstanceEventFactory E =
      new WorkflowInstanceEventFactory(WORKFLOW_INSTANCE);

  private ExecutionDescription desc(String dockerImage) {
    return ExecutionDescription.create(
        dockerImage, Collections.emptyList(), Optional.empty(), Optional.empty());
  }

  @Test
  public void testHaltEventDoesNotRequireExecutionAndGoesStraightToComplete() throws Exception {
    long c = 0L;
    List<SequenceEvent> events = Arrays.asList(
        SequenceEvent.create(E.triggerExecution("trig-0"), c++, ts("07:55")),
        SequenceEvent.create(E.halt(), c++, ts("07:55"))
    );
    assertValidTransitionSequence(events);
    WorkflowInstanceExecutionData workflowInstanceExecutionData =
    new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            Collections.singletonList(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    true,
                    Collections.emptyList()
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testGeneralExample() throws Exception {
    long c = 0L;
    List<SequenceEvent> events = Arrays.asList(
        SequenceEvent.create(E.triggerExecution("trig-0"), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1")), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.terminate(RunState.MISSING_DEPS_EXIT_CODE), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.retry(), c++, ts("08:56")),
        SequenceEvent.create(E.submit(desc("img2")), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57")),
        SequenceEvent.create(E.terminate(0), c++, ts("08:58")),
        SequenceEvent.create(E.success(), c++, ts("08:59")),

        SequenceEvent.create(E.triggerExecution("trig-1"), c++, ts("09:55")),
        SequenceEvent.create(E.dequeue(), c++, ts("09:55")),
        SequenceEvent.create(E.submit(desc("img3")), c++, ts("09:55")),
        SequenceEvent.create(E.submitted("exec-id-10"), c++, ts("09:56")),
        SequenceEvent.create(E.started(), c++, ts("09:57")),
        SequenceEvent.create(E.terminate(1), c++, ts("09:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("09:59")),

        SequenceEvent.create(E.retry(), c++, ts("10:56")),
        SequenceEvent.create(E.submit(desc("img4")), c++, ts("10:55")),
        SequenceEvent.create(E.submitted("exec-id-11"), c++, ts("10:56")),
        SequenceEvent.create(E.started(), c++, ts("10:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            Arrays.asList(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    true,
                    Arrays.asList(
                        Execution.create(
                            "exec-id-00",
                            "img1",
                            Arrays.asList(
                                ExecStatus.create(time("07:56"), "SUBMITTED"),
                                ExecStatus.create(time("07:57"), "STARTED"),
                                ExecStatus.create(time("07:58"), "MISSING_DEPS")
                            )
                        ),
                        Execution.create(
                            "exec-id-01",
                            "img2",
                            Arrays.asList(
                                ExecStatus.create(time("08:56"), "SUBMITTED"),
                                ExecStatus.create(time("08:57"), "STARTED"),
                                ExecStatus.create(time("08:58"), "SUCCESS")
                            )
                        )
                    )
                ),
                Trigger.create(
                    "trig1",
                    time("09:55"),
                    false,
                    Arrays.asList(
                        Execution.create(
                            "exec-id-10",
                            "img3",
                            Arrays.asList(
                                ExecStatus.create(time("09:56"), "SUBMITTED"),
                                ExecStatus.create(time("09:57"), "STARTED"),
                                ExecStatus.create(time("09:58"), "FAILED")
                            )
                        ),
                        Execution.create(
                            "exec-id-11",
                            "img4",
                            Arrays.asList(
                                ExecStatus.create(time("10:56"), "SUBMITTED"),
                                ExecStatus.create(time("10:57"), "STARTED")
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testTimeout() throws Exception {
    long c = 0L;
    List<SequenceEvent> events = Arrays.asList(
        SequenceEvent.create(E.triggerExecution("trig-0"), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1")), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.timeout(), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.retry(), c++, ts("08:56")),
        SequenceEvent.create(E.submit(desc("img2")), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            Collections.singletonList(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    false,
                    Arrays.asList(
                        Execution.create(
                            "exec-id-00",
                            "img1",
                            Arrays.asList(
                                ExecStatus.create(time("07:56"), "SUBMITTED"),
                                ExecStatus.create(time("07:57"), "STARTED"),
                                ExecStatus.create(time("07:58"), "TIMEOUT")
                            )
                        ),
                        Execution.create(
                            "exec-id-01",
                            "img2",
                            Arrays.asList(
                                ExecStatus.create(time("08:56"), "SUBMITTED"),
                                ExecStatus.create(time("08:57"), "STARTED")
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testRunError() throws Exception {
    long c = 0L;
    List<SequenceEvent> events = Arrays.asList(
        SequenceEvent.create(E.triggerExecution("trig-0"), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1")), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.runError("Something failed"), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.retry(), c++, ts("08:56")),
        SequenceEvent.create(E.submit(desc("img2")), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            Collections.singletonList(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    false,
                    Arrays.asList(
                        Execution.create(
                            "exec-id-00",
                            "img1",
                            Arrays.asList(
                                ExecStatus.create(time("07:56"), "SUBMITTED"),
                                ExecStatus.create(time("07:57"), "STARTED"),
                                ExecStatus.create(time("07:58"), "Something failed")
                            )
                        ),
                        Execution.create(
                            "exec-id-01",
                            "img2",
                            Arrays.asList(
                                ExecStatus.create(time("08:56"), "SUBMITTED"),
                                ExecStatus.create(time("08:57"), "STARTED")
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testHaltAndReTrigger() throws Exception {
    long c = 0L;
    List<SequenceEvent> events = Arrays.asList(
        SequenceEvent.create(E.triggerExecution("trig-0"), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1")), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.halt(), c++, ts("07:57")),

        SequenceEvent.create(E.triggerExecution("trig-1"), c++, ts("08:56")),
        SequenceEvent.create(E.dequeue(), c++, ts("08:56")),
        SequenceEvent.create(E.submit(desc("img2")), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-10"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            Arrays.asList(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    true,
                    Collections.singletonList(
                        Execution.create(
                            "exec-id-00",
                            "img1",
                            Arrays.asList(
                                ExecStatus.create(time("07:56"), "SUBMITTED"),
                                ExecStatus.create(time("07:57"), "HALTED")
                            )
                        )
                    )
                ),
                Trigger.create(
                    "trig1",
                    time("08:56"),
                    false,
                    Collections.singletonList(
                        Execution.create(
                            "exec-id-10",
                            "img2",
                            Arrays.asList(
                                ExecStatus.create(time("08:56"), "SUBMITTED"),
                                ExecStatus.create(time("08:57"), "STARTED")
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  private void assertValidTransitionSequence(List<SequenceEvent> events) {
    RunState runState = RunState.fresh(WORKFLOW_INSTANCE, (Time) Instant::now);

    if (!EventUtil.name(events.get(0).event()).equals("triggerExecution")) {
      fail("first event must be triggerExecution");
    }

    for(SequenceEvent event : events) {
      if ("triggerExecution".equals(EventUtil.name(event.event()))
          && runState.state() != RunState.State.NEW) {
        assertThat(runState.state().isTerminal(), is(true));
        runState = RunState.fresh(WORKFLOW_INSTANCE, (Time) Instant::now);
      }

      runState = runState.transition(event.event());
    }
  }

  private static long ts(String time) {
    return time(time).toEpochMilli();
  }

  private static Instant time(String time) {
    return Instant.parse("2016-08-03T" + time + ":03.607Z");
  }
}
