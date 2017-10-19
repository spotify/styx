/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.waitAtMost;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.publisher.EventConsumer;
import java.util.List;
import org.junit.Test;

public class QueuedEventConsumerTest {
  private List<SequenceEvent> trackedEvents = Lists.newArrayList();
  private QueuedEventConsumer consumer = new QueuedEventConsumer(new InjectingEventConsumer());
  private final static WorkflowInstance wfi = WorkflowInstance.create(
      WorkflowId.create("comp1", "work1"),
      "2017-01-01");
  private final static SequenceEvent firstEvent = SequenceEvent.create(
      Event.triggerExecution(wfi, Trigger.natural()), 0, 0);
  private final static SequenceEvent secondEvent = SequenceEvent.create(
      Event.dequeue(wfi), 0, 0);

  @Test
  public void shouldConsumeEvent() throws Exception {
    consumer.enqueue(firstEvent);
    await().atMost(5, SECONDS).until(() -> trackedEvents.get(0) != null);
    assertThat(trackedEvents.get(0), is(firstEvent));
  }

  @Test
  public void shouldSkipEventIfExceptionFromEventConsumer() throws Exception {
    QueuedEventConsumer eventConsumer =
        new QueuedEventConsumer(new ExceptionalEventConsumer());

    eventConsumer.enqueue(firstEvent);
    waitAtMost(5, SECONDS).until(() -> eventConsumer.queueSize() == 0);
    assertThat(trackedEvents.size(), is(0));
  }

  @Test
  public void shouldConsumeMoreEvents() throws Exception {
    consumer.enqueue(firstEvent);
    consumer.enqueue(secondEvent);
    await().atMost(5, SECONDS).until(() -> trackedEvents.get(0) != null);
    await().atMost(5, SECONDS).until(() -> trackedEvents.get(1) != null);
    assertThat(trackedEvents.get(0), is(firstEvent));
    assertThat(trackedEvents.get(1), is(secondEvent));
  }

  @Test(expected = QueuedEventConsumer.IsClosed.class)
  public void shouldRejectEventIfClosed() throws Exception {
    consumer.close();
    consumer.enqueue(firstEvent);
  }

  @Test
  public void ShouldCloseGracefully() throws Exception {
    QueuedEventConsumer eventConsumer =
        new QueuedEventConsumer(new SlowInjectingEventConsumer());

    eventConsumer.enqueue(firstEvent);
    eventConsumer.close();
    assertThat(trackedEvents.get(0), is(firstEvent));
  }

  private class InjectingEventConsumer implements EventConsumer {
    @Override
    public void event(SequenceEvent sequenceEvent) {
      trackedEvents.add(sequenceEvent);
    }
  }

  private class SlowInjectingEventConsumer implements EventConsumer {
    @Override
    public void event(SequenceEvent sequenceEvent) {
      try {
        //Todo better
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      trackedEvents.add(sequenceEvent);
    }
  }

  private class ExceptionalEventConsumer implements EventConsumer {
    @Override
    public void event(SequenceEvent sequenceEvent) {
      try {
        //Todo better
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      throw new RuntimeException("Error");
    }
  }
}
