/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.docker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

import com.google.common.collect.Maps;
import com.spotify.styx.state.RunState;
import java.util.Map;
import java.util.function.Function;
import org.mockito.Mock;

public abstract class AbstractRoutingRunnerTest<T> {

  private int createCounter = 0;
  protected final Map<String, T> createdRunners = Maps.newHashMap();

  @Mock protected Function<RunState, String> runnerId;
  @Mock protected RunState runState;

  protected abstract T mockRunner();

  protected T create(String id) {
    var mock = mockRunner();
    createCounter++;
    createdRunners.put(id, mock);
    return mock;
  }

  protected void assertThatCreateCountersContains(String... ids) {
    assertThat(createCounter, equalTo(ids.length));
    assertThat(createdRunners.keySet(), hasSize(ids.length));
    for (var id : ids) {
      assertThat(createdRunners, hasKey(id));
    }
  }
}
