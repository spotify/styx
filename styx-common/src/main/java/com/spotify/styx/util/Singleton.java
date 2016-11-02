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

import static java.util.Objects.requireNonNull;

import com.spotify.apollo.Environment;
import java.util.function.Function;

/**
 * A wrapper for a provider function which ensures the provider is only invoked once.
 *
 * <p>This class is not thread safe and should only be used from one thread.
 */
public final class Singleton<T> implements Function<Environment, T> {

  private final Function<Environment, T> provider;
  private T singleton;

  private Singleton(Function<Environment, T> provider) {
    this.provider = requireNonNull(provider);
  }

  public static <T> Singleton<T> create(Function<Environment, T> provider) {
    return new Singleton<>(provider);
  }

  @Override
  public T apply(Environment environment) {
    if (singleton != null) {
      return singleton;
    }

    singleton = provider.apply(environment);
    return singleton;
  }
}
