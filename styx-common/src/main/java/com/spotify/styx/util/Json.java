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

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY;
import static com.fasterxml.jackson.databind.PropertyNamingStrategy.SNAKE_CASE;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spotify.styx.state.PersistentTrigger;
import com.spotify.styx.state.Trigger;
import io.norberg.automatter.jackson.AutoMatterModule;

public final class Json {

  private Json() {
  }

  static final TypeWrapperModule ADT_MODULE = new TypeWrapperModule()
      .setupWrapping(
          Trigger.class,
          PersistentTrigger.class,
          PersistentTrigger::wrap,
          PersistentTrigger::toTrigger);

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(SNAKE_CASE)
      .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY)
      .disable(WRITE_DATES_AS_TIMESTAMPS)
      .registerModule(ADT_MODULE)
      .registerModule(new JavaTimeModule())
      .registerModule(new Jdk8Module())
      .registerModule(new AutoMatterModule());
}
