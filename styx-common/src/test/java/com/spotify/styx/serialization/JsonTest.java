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

package com.spotify.styx.serialization;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.spotify.styx.model.Event;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import javaslang.control.Try.CheckedFunction;
import org.junit.Test;

public class JsonTest {

  private static final Event EVENT = Event.retry(TestData.WORKFLOW_INSTANCE);
  private static final Trigger TRIGGER = Trigger.adhoc("foobar");

  @Test
  public void jsonMapperShouldIgnoreUnknownProperties() throws Throwable {
    assertIgnoresUnknownProperties(EVENT, node -> Json.OBJECT_MAPPER.convertValue(node, Event.class));
  }

  @Test
  public void yamlMapperShouldIgnoreUnknownProperties() throws Throwable {
    assertIgnoresUnknownProperties(EVENT, node -> Json.YAML_MAPPER.convertValue(node, Event.class));
  }

  @Test
  public void deserializeEventShouldIgnoreUnknownProperties() throws Throwable {
    assertIgnoresUnknownProperties(EVENT, node -> Json.deserializeEvent(Json.serialize(node)));
  }

  @Test
  public void deserializeTriggerShouldIgnoreUnknownProperties() throws Throwable {
    assertIgnoresUnknownProperties(TRIGGER, node -> Json.deserializeTrigger(Json.serialize(node)));
  }

  @Test
  public void deserializeShouldIgnoreUnknownProperties() throws Throwable {
    assertIgnoresUnknownProperties(EVENT, node -> Json.deserialize(Json.serialize(node), Event.class));
  }

  private <T> void assertIgnoresUnknownProperties(T value, CheckedFunction<ObjectNode, T> deserialize)
      throws Throwable {
    final ObjectNode node = Json.OBJECT_MAPPER.convertValue(value, ObjectNode.class);
    node.put("non_existent_field", "foobar");
    final T deserialized = deserialize.apply(node);
    assertThat(deserialized, is(value));
  }
}
