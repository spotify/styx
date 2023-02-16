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

package com.spotify.styx.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import org.junit.Test;

public class EventUtilTest {

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  @Test
  public void infoTriggerExecution() {
    assertThat(
        EventUtil.info(Event.triggerExecution(TestData.WORKFLOW_INSTANCE, Trigger.adhoc("foobar"), TRIGGER_PARAMETERS)),
        is("Trigger id: foobar, Parameters: {\"env\":{\"BAR\":\"bar\",\"FOO\":\"foo\"}}"));
  }

  // TODO: test the rest of EventUtil
}
