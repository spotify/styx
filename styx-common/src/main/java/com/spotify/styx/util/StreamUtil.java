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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class StreamUtil {

  private StreamUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Concatenate an array of streams into one stream.
   *
   * @param streams  The streams to concatenate
   * @param <T>      The type of the stream elements
   * @return A stream of all the individual streams
   */
  @SafeVarargs
  public static <T> Stream<T> cat(Stream<T>... streams) {
    return Arrays.stream(streams).reduce(Stream.empty(), Stream::concat);
  }

  public static <T> Stream<T> stream(Iterator<T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
  }
}
