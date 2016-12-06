/*-
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

package com.spotify.styx.state;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.StyxScheduler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.junit.Test;

public class TimeoutConfigTest {

  @Test
  public void testDurations() throws Exception {
    Config config = ConfigFactory.load("ttl");
    Config ttls = config.getConfig(StyxScheduler.STYX_STALE_STATE_TTL_CONFIG);

    TimeoutConfig timeouts = TimeoutConfig.createFromConfig(ttls);

    assertThat(timeouts.ttlOf(RunState.State.RUNNING), is(Duration.ofHours(24)));
    assertThat(timeouts.ttlOf(RunState.State.SUBMITTED), is(Duration.ofMinutes(5)));
    assertThat(timeouts.ttlOf(RunState.State.TERMINATED), is(Duration.ofDays(2)));
    assertThat(timeouts.ttlOf(RunState.State.QUEUED), is(Duration.ofHours(8)));
    assertThat(timeouts.ttlOf(RunState.State.NEW), is(Duration.ofDays(2)));
  }
}
