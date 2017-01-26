/*- 
 * -\-\- 
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

import okio.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class TriggerSerializerTest {

  private static final String TRIGGER_ID = "trig";

  private TriggerSerializer triggerSerializer = new TriggerSerializer();

  @Test
  public void testRoundtripAllEvents() {
    assertRoundtrip(Trigger.natural());
    assertRoundtrip(Trigger.adhoc(TRIGGER_ID));
    assertRoundtrip(Trigger.backfill(TRIGGER_ID));
    assertRoundtrip(Trigger.unknown(TRIGGER_ID));
  }

  @Test
  public void testDeserializeFromJson() throws Exception {
    assertThat(triggerSerializer.deserialize(json("natural")), is(Trigger.natural()));
    assertThat(
        triggerSerializer.deserialize(json("adhoc", TRIGGER_ID)),
        is(Trigger.adhoc(TRIGGER_ID)));
    assertThat(
        triggerSerializer.deserialize(json("backfill", TRIGGER_ID)),
        is(Trigger.backfill(TRIGGER_ID)));
    assertThat(
        triggerSerializer.deserialize(json("unknown", TRIGGER_ID)),
        is(Trigger.unknown(TRIGGER_ID)));
  }

  @Test
  public void testNaturalTrigger() {
    ByteString byteString = triggerSerializer.serialize(Trigger.natural());
    assertThat(byteString, is(json("natural")));
  }

  private void assertRoundtrip(Trigger trigger) {
    ByteString byteString = triggerSerializer.serialize(trigger);
    Trigger deserializedTrigger = triggerSerializer.deserialize(byteString);
    Assert.assertThat(
        "serialized trigger did not match actual trigger after deserialization: " + byteString.utf8(),
        deserializedTrigger, is(trigger));
  }

  private ByteString json(String triggerType) {
    return ByteString.encodeUtf8(String.format(
        "{\"@type\":\"%s\"}",
        triggerType));
  }

  private ByteString json(String triggerType, String triggerId) {
    return ByteString.encodeUtf8(String.format(
        "{\"@type\":\"%s\",\"trigger_id\":\"%s\"}",
        triggerType, triggerId));
  }
}
