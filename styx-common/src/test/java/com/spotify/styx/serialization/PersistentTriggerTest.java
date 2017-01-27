/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.serialization;

import static com.spotify.styx.serialization.Json.deserializeTrigger;
import static com.spotify.styx.serialization.Json.serialize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.state.Trigger;
import okio.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class PersistentTriggerTest {

  private static final String TRIGGER_ID = "trig";

  @Test
  public void testRoundtripAllEvents() throws Exception {
    assertRoundtrip(Trigger.natural());
    assertRoundtrip(Trigger.adhoc(TRIGGER_ID));
    assertRoundtrip(Trigger.backfill(TRIGGER_ID));
    assertRoundtrip(Trigger.unknown(TRIGGER_ID));
  }

  @Test
  public void testDeserializeFromJson() throws Exception {
    assertThat(
        deserializeTrigger(jsonNatural()),
        is(Trigger.natural()));
    assertThat(
        deserializeTrigger(json("adhoc", TRIGGER_ID)),
        is(Trigger.adhoc(TRIGGER_ID)));
    assertThat(
        deserializeTrigger(json("backfill", TRIGGER_ID)),
        is(Trigger.backfill(TRIGGER_ID)));
    assertThat(
        deserializeTrigger(json("unknown", TRIGGER_ID)),
        is(Trigger.unknown(TRIGGER_ID)));
  }

  @Test
  public void testNaturalTrigger() throws Exception {
    ByteString byteString = serialize(Trigger.natural());
    assertThat(byteString, is(jsonNatural()));
  }

  private void assertRoundtrip(Trigger trigger) throws Exception {
    ByteString byteString = serialize(trigger);
    Trigger deserializedTrigger = deserializeTrigger(byteString);
    Assert.assertThat(
        "serialized trigger did not match actual trigger after deserialization: " + byteString.utf8(),
        deserializedTrigger, is(trigger));
  }

  private ByteString jsonNatural() {
    return ByteString.encodeUtf8("{\"@type\":\"natural\"}");
  }

  private ByteString json(String triggerType, String triggerId) {
    return ByteString.encodeUtf8(String.format(
        "{\"@type\":\"%s\",\"trigger_id\":\"%s\"}",
        triggerType, triggerId));
  }
}
