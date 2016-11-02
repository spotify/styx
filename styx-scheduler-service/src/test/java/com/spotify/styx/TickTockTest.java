/*
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

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.spotify.styx.FakeScheduledExecutorService.DelayedTask;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.testdata.TestData;
import java.net.URI;
import java.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TickTockTest {

  private static final Logger LOG = LoggerFactory.getLogger(TickTockTest.class);

  private static final String ID_1 = "id1";

  private Instant time;

  private FakeScheduledExecutorService exec = new FakeScheduledExecutorService();
  private Workflow triggeredWorkflow;
  private String triggerId;
  private Instant triggeredTime;

  private void trigger(Workflow workflow, String triggerId, Instant instant) {
    LOG.info("triggered instant = {}", instant);
    this.triggeredWorkflow = workflow;
    this.triggerId = triggerId;
    this.triggeredTime = instant;
  }

  private TickTock createTickTock(DataEndpoint dataEndpoint) {
    Workflow workflow = Workflow.create(ID_1, URI.create("http://example.com"), dataEndpoint);
    return new TickTock(workflow, this::trigger, () -> time, exec);
  }

  @Test
  public void shouldTriggerWithNaturalId() throws Exception {
    TickTock tickTock = createTickTock(TestData.HOURLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59.500Z");
    tickTock.start();

    exec.lastSubmittedTask().runnable().run();
    assertThat(triggerId, is("natural"));
  }

  @Test
  public void shouldTriggerOnHour() throws Exception {
    TickTock tickTock = createTickTock(TestData.HOURLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59.500Z");
    tickTock.start();

    exec.lastSubmittedTask().runnable().run();
    assertThat(triggeredWorkflow.componentId(), is(ID_1));
    assertThat(triggeredWorkflow.schedule().id(), is(TestData.HOURLY_DATA_ENDPOINT.id()));
    assertThat(triggeredTime, is(Instant.parse("2015-12-31T23:00:00Z")));
  }

  @Test
  public void shouldTriggerOnDay() throws Exception {
    TickTock tickTock = createTickTock(TestData.DAILY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59.500Z");
    tickTock.start();

    exec.lastSubmittedTask().runnable().run();
    assertThat(triggeredWorkflow.componentId(), is(ID_1));
    assertThat(triggeredWorkflow.schedule().id(), is(TestData.DAILY_DATA_ENDPOINT.id()));
    assertThat(triggeredTime, is(Instant.parse("2015-12-31T00:00:00Z")));
  }

  @Test
  public void shouldTriggerOnWeek() throws Exception {
    TickTock tickTock = createTickTock(TestData.WEEKLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-27T23:59:58.500Z");
    tickTock.start();

    exec.lastSubmittedTask().runnable().run();
    assertThat(triggeredWorkflow.componentId(), is(ID_1));
    assertThat(triggeredWorkflow.endpointId(), is(TestData.WEEKLY_DATA_ENDPOINT.id()));
    assertThat(triggeredTime, is(Instant.parse("2015-12-21T00:00:00Z")));
  }

  @Test
  public void shouldHaveScheduledTriggerForNextPeriodHourly() throws Exception {
    TickTock tickTock = createTickTock(TestData.HOURLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59.500Z");
    tickTock.start();

    DelayedTask delayedTask1 = exec.lastSubmittedTask();
    assertThat(delayedTask1.delay(), is(delayedTask1.unit().convert(500, MILLISECONDS)));

    time = Instant.parse("2016-01-01T00:00:00Z");
    delayedTask1.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2015-12-31T23:00:00Z")));

    DelayedTask delayedTask2 = exec.lastSubmittedTask();
    assertThat(delayedTask2.delay(), is(delayedTask2.unit().convert(1, HOURS)));

    time = Instant.parse("2016-01-01T01:00:00Z");
    delayedTask2.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2016-01-01T00:00:00Z")));

    DelayedTask delayedTask3 = exec.lastSubmittedTask();
    assertThat(delayedTask3.delay(), is(delayedTask3.unit().convert(1, HOURS)));
  }

  @Test
  public void shouldHaveScheduledTriggerForNextPeriodDaily() throws Exception {
    TickTock tickTock = createTickTock(TestData.DAILY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59.500Z");
    tickTock.start();

    DelayedTask delayedTask1 = exec.lastSubmittedTask();
    assertThat(delayedTask1.delay(), is(delayedTask1.unit().convert(500, MILLISECONDS)));

    time = Instant.parse("2016-01-01T00:00:00Z");
    delayedTask1.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2015-12-31T00:00:00Z")));

    DelayedTask delayedTask2 = exec.lastSubmittedTask();
    assertThat(delayedTask2.delay(), is(delayedTask2.unit().convert(1, DAYS)));

    time = Instant.parse("2016-01-02T00:00:00Z");
    delayedTask2.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2016-01-01T00:00:00Z")));

    DelayedTask delayedTask3 = exec.lastSubmittedTask();
    assertThat(delayedTask3.delay(), is(delayedTask3.unit().convert(1, DAYS)));
  }

  @Test
  public void shouldHaveScheduledTriggerForNextPeriodWeekly() throws Exception {
    TickTock tickTock = createTickTock(TestData.WEEKLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-27T23:59:59.500Z");
    tickTock.start();

    DelayedTask delayedTask1 = exec.lastSubmittedTask();
    assertThat(delayedTask1.delay(), is(delayedTask1.unit().convert(500, MILLISECONDS)));

    time = Instant.parse("2015-12-28T00:00:00Z");
    delayedTask1.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2015-12-21T00:00:00Z")));

    DelayedTask delayedTask2 = exec.lastSubmittedTask();
    assertThat(delayedTask2.delay(), is(delayedTask2.unit().convert(7, DAYS)));

    time = Instant.parse("2016-01-04T00:00:00Z");
    delayedTask2.runnable().run();
    assertThat(triggeredTime, is(Instant.parse("2015-12-28T00:00:00Z")));

    DelayedTask delayedTask3 = exec.lastSubmittedTask();
    assertThat(delayedTask3.delay(), is(delayedTask3.unit().convert(7, DAYS)));
  }

  @Test
  public void shouldScheduleWithDelay() throws Exception {
    TickTock tickTock = createTickTock(TestData.HOURLY_DATA_ENDPOINT);

    time = Instant.parse("2016-01-01T00:59:00Z");
    tickTock.start();

    DelayedTask delayedTask = exec.lastSubmittedTask();
    assertThat(delayedTask.delay(), is(delayedTask.unit().convert(1, MINUTES)));
  }

  @Test
  public void shouldNotTriggerWhenClosed() throws Exception {
    TickTock tickTock = createTickTock(TestData.HOURLY_DATA_ENDPOINT);

    time = Instant.parse("2015-12-31T23:59:59Z");
    tickTock.start();
    tickTock.close();

    exec.lastSubmittedTask().runnable().run();
    assertThat(triggeredWorkflow, is(nullValue()));
    assertThat(exec.allTasks(), is(empty()));
  }
}
