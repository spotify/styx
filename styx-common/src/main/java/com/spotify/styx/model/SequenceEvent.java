/*
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
package com.spotify.styx.model;

import com.google.auto.value.AutoValue;
import java.util.Comparator;

@AutoValue
public abstract class SequenceEvent {

  public static final Comparator<SequenceEvent> COUNTER_COMPARATOR =
      (a, b) -> (int) (a.counter() - b.counter());

  public abstract Event event();
  public abstract long counter();
  public abstract long timestamp();

  public static SequenceEvent create(Event event, long counter, long timestamp) {
    return new AutoValue_SequenceEvent(event, counter, timestamp);
  }

  public static SequenceEvent parseKey(String key, Event event, long timestamp) {
    final int lastHashPos = key.lastIndexOf('#');
    if (lastHashPos < 1) {
      throw new IllegalArgumentException("Key must contain a hash '#' sign on position > 0");
    }

    final long counter = Long.parseLong(key.substring(lastHashPos + 1));
    return create(event, counter, timestamp);
  }
}
