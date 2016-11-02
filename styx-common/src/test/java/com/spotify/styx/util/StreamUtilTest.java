/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class StreamUtilTest {

  @Test
  public void testCatSingle() throws Exception {
    Stream<Integer> input = Stream.of(1, 2);
    List<Integer> output = StreamUtil.cat(input).collect(Collectors.toList());

    assertThat(output, contains(1, 2));
  }

  @Test
  public void testCatTwo() throws Exception {
    Stream<Integer> input1 = Stream.of(1, 2);
    Stream<Integer> input2 = Stream.of(3, 4);
    List<Integer> output = StreamUtil.cat(input1, input2).collect(Collectors.toList());

    assertThat(output, contains(1, 2, 3, 4));
  }

  @Test
  public void testCatMultiple() throws Exception {
    Stream<Integer> input1 = Stream.of(1, 2);
    Stream<Integer> input2 = Stream.of(3, 4);
    Stream<Integer> input3 = Stream.of(5, 6);
    List<Integer> output = StreamUtil.cat(input1, input2, input3).collect(Collectors.toList());

    assertThat(output, contains(1, 2, 3, 4, 5, 6));
  }
}
