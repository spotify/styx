/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.storage;

import static com.spotify.styx.WorkflowInstanceEventFactory.TEST_RUNNER_ID;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.spotify.styx.WorkflowInstanceEventFactory;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.ExecStatus;
import com.spotify.styx.model.data.Execution;
import com.spotify.styx.model.data.Trigger;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.EventUtil;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public class WFIExecutionBuilderTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.parseKey("component1#endpoint1#2016-08-03T06");
  private static final WorkflowInstanceEventFactory E =
      new WorkflowInstanceEventFactory(WORKFLOW_INSTANCE);
  private static final com.spotify.styx.state.Trigger
      UNKNOWN_TRIGGER0 = com.spotify.styx.state.Trigger.unknown("trig0");
  private static final com.spotify.styx.state.Trigger
      UNKNOWN_TRIGGER1 = com.spotify.styx.state.Trigger.unknown("trig1");
  private static final Set<String> RESOURCE_IDS = Set.of("foo-resource", "bar-resource");

  private ExecutionDescription desc(String dockerImage, String commitSha) {
    return ExecutionDescription.builder()
        .dockerImage(dockerImage)
        .commitSha(commitSha)
        .build();
  }

  private ExecutionDescription desc(String dockerImage) {
    return ExecutionDescription.builder()
        .dockerImage(dockerImage)
        .build();
  }

  @Test
  public void testHaltEventAfterTriggerEvent() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.halt(), c++, ts("07:56"))
    );
    assertValidTransitionSequence(events);
    WorkflowInstanceExecutionData workflowInstanceExecutionData =
    new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    true,
                    List.of(
                        Execution.create(
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of(
                                ExecStatus.create(time("07:56"), "HALTED", Optional.empty())
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testRunErrorEventAfterTriggerEvent() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.runError("Error message"), c++, ts("07:56"))
    );
    assertValidTransitionSequence(events);
    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            List.of(
                                ExecStatus.create(time("07:56"), "FAILED", Optional.of("Error message"))
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testGeneralExample() {
    long c = 0L;
    TriggerParameters triggerParameters = TriggerParameters.builder()
        .env("FOO", "foo",
            "BAR", "bar")
        .build();
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0, triggerParameters), c++, ts("07:55")),
        SequenceEvent.create(E.info(Message.info("foo bar")), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.terminate(RunState.MISSING_DEPS_EXIT_CODE), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("08:54")),
        SequenceEvent.create(E.submit(desc("img2"), "exec-id-01"), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57")),
        SequenceEvent.create(E.terminate(0), c++, ts("08:58")),
        SequenceEvent.create(E.success(), c++, ts("08:59")),

        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER1), c++, ts("09:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("09:55")),
        SequenceEvent.create(E.submit(desc("img3", "sha3"), "exec-id-10"), c++, ts("09:55")),
        SequenceEvent.create(E.submitted("exec-id-10"), c++, ts("09:56")),
        SequenceEvent.create(E.started(), c++, ts("09:57")),
        SequenceEvent.create(E.terminate(1), c++, ts("09:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("09:59")),

        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("10:54")),
        SequenceEvent.create(E.submit(desc("img4", "sha4"), "exec-id-11"), c++, ts("10:55")),
        SequenceEvent.create(E.submitted("exec-id-11"), c++, ts("10:56")),
        SequenceEvent.create(E.started(), c++, ts("10:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    triggerParameters,
                    true,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("07:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("07:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("07:58"), "MISSING_DEPS", Optional.empty())
                            )
                        ),
                        Execution.create(
                            Optional.of("exec-id-01"),
                            Optional.of("img2"),
                            Optional.empty(),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("08:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("08:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("08:58"), "SUCCESS", Optional.empty())
                            )
                        )
                    )
                ),
                Trigger.create(
                    "trig1",
                    time("09:55"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-10"),
                            Optional.of("img3"),
                            Optional.of("sha3"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("09:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("09:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("09:58"), "FAILED", Optional.of("Exit code: 1"))
                            )
                        ),
                        Execution.create(
                            Optional.of("exec-id-11"),
                            Optional.of("img4"),
                            Optional.of("sha4"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("10:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("10:57"), "STARTED", Optional.empty())
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testFailureNoExitCode() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.terminate(Optional.empty()), c++, ts("07:58"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("07:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("07:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("07:58"), "FAILED", Optional.of("Exit code unknown"))
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testTimeout() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57")),
        SequenceEvent.create(E.timeout(), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("08:54")),
        SequenceEvent.create(E.submit(desc("img2", "sha2"), "exec-id-01"), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("07:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("07:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("07:58"), "TIMEOUT", Optional.empty())
                            )
                        ),
                        Execution.create(
                            Optional.of("exec-id-01"),
                            Optional.of("img2"),
                            Optional.of("sha2"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("08:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("08:57"), "STARTED", Optional.empty())
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testReceiveTimeoutWithMissingTriggerExecution() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.timeout(), c++, ts("07:54")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.started(), c++, ts("07:57"))
    );

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "UNKNOWN",
                    time("07:54"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("07:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("07:57"), "STARTED", Optional.empty())
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testRunError() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.runError("First failure"), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("08:54")),
        SequenceEvent.create(E.submit(desc("img2", "sha2"), "exec-id-01"), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57")),
        SequenceEvent.create(E.runError("Second failure"), c++, ts("08:59"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.empty(),
                            List.of(
                                ExecStatus.create(time("07:58"), "FAILED", Optional.of("First failure"))
                            )
                        ),
                        Execution.create(
                            Optional.of("exec-id-01"),
                            Optional.of("img2"),
                            Optional.of("sha2"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("08:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("08:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("08:59"), "FAILED", Optional.of("Second failure"))
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testHaltAndReTrigger() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.submitted("exec-id-00"), c++, ts("07:56")),
        SequenceEvent.create(E.halt(), c++, ts("07:57")),

        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER1), c++, ts("08:56")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("08:56")),
        SequenceEvent.create(E.submit(desc("img2", "sha2"), "exec-id-10"), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-10"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    true,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("07:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("07:57"), "HALTED", Optional.empty())
                            )
                        )
                    )
                ),
                Trigger.create(
                    "trig1",
                    time("08:56"),
                    TriggerParameters.zero(),
                    false,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-10"),
                            Optional.of("img2"),
                            Optional.of("sha2"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("08:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("08:57"), "STARTED", Optional.empty())
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  @Test
  public void testStop() {
    long c = 0L;
    List<SequenceEvent> events = List.of(
        SequenceEvent.create(E.triggerExecution(UNKNOWN_TRIGGER0), c++, ts("07:55")),
        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("07:55")),
        SequenceEvent.create(E.submit(desc("img1", "sha1"), "exec-id-00"), c++, ts("07:55")),
        SequenceEvent.create(E.runError("First failure"), c++, ts("07:58")),
        SequenceEvent.create(E.retryAfter(10), c++, ts("07:59")),

        SequenceEvent.create(E.dequeue(RESOURCE_IDS), c++, ts("08:54")),
        SequenceEvent.create(E.submit(desc("img2", "sha2"), "exec-id-01"), c++, ts("08:55")),
        SequenceEvent.create(E.submitted("exec-id-01"), c++, ts("08:56")),
        SequenceEvent.create(E.started(), c++, ts("08:57")),
        SequenceEvent.create(E.runError("Second failure"), c++, ts("08:59")),
        SequenceEvent.create(E.stop(), c++, ts("08:59"))
    );
    assertValidTransitionSequence(events);

    WorkflowInstanceExecutionData workflowInstanceExecutionData =
        new WFIExecutionBuilder().executionInfo(events);
    WorkflowInstanceExecutionData expected =
        WorkflowInstanceExecutionData.create(
            WORKFLOW_INSTANCE,
            List.of(
                Trigger.create(
                    "trig0",
                    time("07:55"),
                    TriggerParameters.zero(),
                    true,
                    List.of(
                        Execution.create(
                            Optional.of("exec-id-00"),
                            Optional.of("img1"),
                            Optional.of("sha1"),
                            Optional.empty(),
                            List.of(
                                ExecStatus.create(time("07:58"), "FAILED", Optional.of("First failure"))
                            )
                        ),
                        Execution.create(
                            Optional.of("exec-id-01"),
                            Optional.of("img2"),
                            Optional.of("sha2"),
                            Optional.of(TEST_RUNNER_ID),
                            List.of(
                                ExecStatus.create(time("08:56"), "SUBMITTED", Optional.empty()),
                                ExecStatus.create(time("08:57"), "STARTED", Optional.empty()),
                                ExecStatus.create(time("08:59"), "FAILED", Optional.of("Second failure"))
                            )
                        )
                    )
                )
            )
        );

    assertThat(workflowInstanceExecutionData, is(expected));
  }

  private void assertValidTransitionSequence(List<SequenceEvent> events) {
    RunState runState = RunState.fresh(WORKFLOW_INSTANCE, Instant::now);

    if (!EventUtil.name(events.get(0).event()).equals("triggerExecution")) {
      fail("first event must be triggerExecution");
    }

    for(SequenceEvent event : events) {
      if ("triggerExecution".equals(EventUtil.name(event.event()))
          && runState.state() != RunState.State.NEW) {
        assertThat(runState.state().isTerminal(), is(true));
        runState = RunState.fresh(WORKFLOW_INSTANCE, Instant::now);
      }

      runState = runState.transition(event.event(), Instant::now);
    }
  }

  private static long ts(String time) {
    return time(time).toEpochMilli();
  }

  private static Instant time(String time) {
    return Instant.parse("2016-08-03T" + time + ":03.607Z");
  }
}
