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

package com.spotify.styx.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.spotify.styx.model.ExecutionDescription;
import io.norberg.automatter.AutoMatter;
import java.util.List;
import java.util.Optional;

/**
 * A value type for holding data related to the various states of the {@link RunState}.
 *
 * <p>Uses auto-matter over auto-value because we get a builder by default, which is also better
 * suited for copy-modifying values. Another plus is the boilerplate-free Jackson integration.
 */
@AutoMatter
@JsonIgnoreProperties(ignoreUnknown = true)
public interface StateData {

  int tries();
  double retryCost();
  Optional<Long> retryDelayMillis();
  Optional<Integer> lastExit();
  Optional<String> triggerId();
  Optional<String> executionId();
  Optional<ExecutionDescription> executionDescription();
  List<Message> messages();

  StateDataBuilder builder();

  static StateDataBuilder newBuilder() {
    return new StateDataBuilder();
  }

  static StateData zero() {
    return newBuilder().build();
  }

  @AutoMatter
  interface Message {
    MessageLevel level();
    String line();

    static Message create(MessageLevel level, String line) {
      return new MessageBuilder().level(level).line(line).build();
    }

    static Message info(String line) {
      return create(MessageLevel.INFO, line);
    }

    static Message warning(String line) {
      return create(MessageLevel.WARNING, line);
    }

    static Message error(String line) {
      return create(MessageLevel.ERROR, line);
    }
  }

  enum MessageLevel {
    INFO, WARNING, ERROR, UNKNOWN;

    @JsonCreator
    public static MessageLevel forValue(String value) {
      try {
        return valueOf(value);
      } catch (IllegalArgumentException ignore) {
        return UNKNOWN;
      }
    }
  }
}
