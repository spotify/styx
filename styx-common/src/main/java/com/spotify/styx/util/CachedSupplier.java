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

package com.spotify.styx.util;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.function.Supplier;
import javaslang.control.Try;

/**
 * A decorator for a supplier function that will cache the returned value during some
 * configurable time.
 */
public class CachedSupplier<T> implements Supplier<T> {

  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private final LoadingCache<Boolean, T> cache;

  public CachedSupplier(Try.CheckedSupplier<T> delegate, Time time) {
    this(delegate, time, DEFAULT_TIMEOUT);
  }

  CachedSupplier(Try.CheckedSupplier<T> delegate, Time time, Duration timeout) {
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(timeout)
        .ticker(new Ticker() {
          @Override
          public long read() {
            return time.nanoTime();
          }
        })
        .build(CacheLoader.from(() -> Try.of(delegate).get()));
  }

  @Override
  public T get() {
    return Try.of(() -> cache.get(Boolean.TRUE)).get();
  }
}
