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

package com.spotify.styx.state.handlers;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;

import com.spotify.styx.util.ClassEnforcer;
import java.util.List;
import org.junit.Test;

public class HandlerUtilTest {
  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(HandlerUtil.class), is(true));
  }
  
  @Test
  public void shouldReplaceArgs() {
    assertThat(HandlerUtil.argsReplace(List.of("foo", "bar", "{}"), "foobar"),
        is(List.of("foo", "bar", "foobar")));
  }

  @Test
  public void shouldDoNothingIfNoPlaceholder() {
    assertThat(HandlerUtil.argsReplace(List.of("foo", "bar", "foobar"), "barfoo"),
        is(List.of("foo", "bar", "foobar")));
  }
}
