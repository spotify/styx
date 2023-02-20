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

package com.spotify.styx.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import org.junit.Test;

public class StateDataTest {

  @Test
  public void testMessage() {
    assertThat(StateData.newBuilder().build().message(),
        is(Optional.empty()));

    assertThat(StateData.newBuilder().messages(Message.info("foo")).build().message(),
        is(Optional.of(Message.info("foo"))));

    assertThat(StateData.newBuilder().messages(Message.info("foo"), Message.info("bar")).build().message(),
        is(Optional.of(Message.info("bar"))));
  }
}
