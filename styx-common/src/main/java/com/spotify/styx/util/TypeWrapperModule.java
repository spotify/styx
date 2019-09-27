/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.function.Function;

/**
 * A Jackson module that allows for setting up wrapping classes for types that can not directly
 * be serialized or annotated.
 */
public class TypeWrapperModule extends SimpleModule {

  public TypeWrapperModule() {
    super("AdtModule");
  }

  @Override
  public void setupModule(SetupContext context) {
    super.setupModule(context);
  }

  public <T, W> TypeWrapperModule setupWrapping(
      Class<T> valueClass,
      Class<W> wrapperClass,
      Function<T, W> wrapper,
      Function<W, T> unwrapper) {
    super.addSerializer(valueClass, new DelegatingSerializer<>(valueClass, wrapper));
    super.addDeserializer(valueClass, new DelegatingDeserializer<>(valueClass, wrapperClass, unwrapper));

    return this;
  }

  private static class DelegatingSerializer<T> extends StdSerializer<T> {

    private final Function<T, ?> wrapper;

    private DelegatingSerializer(Class<T> jsonClass, Function<T, ?> wrapper) {
      super(jsonClass);
      this.wrapper = wrapper;
    }

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeObject(wrapper.apply(value));
    }
  }

  private static class DelegatingDeserializer<T, W> extends StdDeserializer<T> {

    private final Class<W> wrapperClass;
    private final Function<W, T> unwrapper;

    private DelegatingDeserializer(Class<T> valueClass, Class<W> wrapperClass, Function<W, T> unwrapper) {
      super(valueClass);
      this.wrapperClass = wrapperClass;
      this.unwrapper = unwrapper;
    }

    @Override
    public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return unwrapper.apply(ctxt.readValue(p, wrapperClass));
    }
  }
}
