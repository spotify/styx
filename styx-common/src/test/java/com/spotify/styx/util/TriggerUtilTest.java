/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.util;

import static com.spotify.styx.util.TriggerUtil.trigger;
import static com.spotify.styx.util.TriggerUtil.triggerType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.state.Trigger;
import org.junit.Test;

public class TriggerUtilTest {

  @Test
  public void testTriggerType() {
    assertThat(triggerType(Trigger.natural()), is("natural"));
    assertThat(triggerType(Trigger.adhoc("foo")), is("adhoc"));
    assertThat(triggerType(Trigger.backfill("bar")), is("backfill"));
    assertThat(triggerType(Trigger.unknown("baz")), is("unknown"));
  }

  @Test
  public void testTrigger() {
    assertThat(trigger("natural", "natural-trigger"), is(Trigger.natural()));
    assertThat(trigger("adhoc", "foo"), is(Trigger.adhoc("foo")));
    assertThat(trigger("backfill", "bar"), is(Trigger.backfill("bar")));
    assertThat(trigger("unknown", "baz"), is(Trigger.unknown("baz")));
  }
}
