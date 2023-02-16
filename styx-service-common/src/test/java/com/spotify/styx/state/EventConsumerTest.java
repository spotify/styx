/*-
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2019 Spotify AB
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;

import com.spotify.styx.model.SequenceEvent;
import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventConsumerTest {
  @Mock private EventConsumer eventConsumer1;
  @Mock private EventConsumer eventConsumer2;
  @Mock private SequenceEvent event;
  @Mock private RunState runState;

  @Test
  public void fanEvent() {
    var eventConsumer = EventConsumer.fanEvent(List.of(eventConsumer1, eventConsumer2));
    eventConsumer.accept(event, runState);
    verify(eventConsumer1).accept(event, runState);
    verify(eventConsumer2).accept(event, runState);
  }

  @Test
  public void tracing() {
    var eventConsumers = EventConsumer.tracing(List.of(eventConsumer1, eventConsumer2));

    assertThat(eventConsumers.size(), is(2));
    assertThat(Proxy.isProxyClass(eventConsumers.get(0).getClass()), is(true));
    assertThat(Proxy.isProxyClass(eventConsumers.get(1).getClass()), is(true));
    eventConsumers.get(0).accept(event, runState);
    verify(eventConsumer1).accept(event, runState);
    eventConsumers.get(1).accept(event, runState);
    verify(eventConsumer2).accept(event, runState);
  }
}
