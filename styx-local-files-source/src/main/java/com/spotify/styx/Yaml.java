/*
 * -\-\-
 * Spotify Styx Local Files Schedule Source
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
package com.spotify.styx;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;

class Yaml {

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
      .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
      .enable(ACCEPT_SINGLE_VALUE_AS_ARRAY)
      .registerModule(new Jdk8Module())
      .registerModule(new JavaTimeModule());

  private Yaml() {
  }

  static YamlScheduleDefinition parseScheduleDefinition(byte[] bytes) throws IOException {
    return YAML_MAPPER.readValue(bytes, YamlScheduleDefinition.class);
  }
}
