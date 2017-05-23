/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.monitoring;

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * A proxy for instrumenting an instance of {@link Storage} or {@link DockerRunner} using
 * {@link Proxy#newProxyInstance}.
 */
public class MeteredProxy implements InvocationHandler {

  private final Object delegate;
  private final Stats stats;
  private final Time time;

  private MeteredProxy(Object delegate, Stats stats, Time time) {
    this.delegate = Objects.requireNonNull(delegate);
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
  }

  @SuppressWarnings("unchecked")
  public static <T> T instrument(Class<? extends T> clazz, T delegate, Stats stats, Time time) {
    final InvocationHandler handler = new MeteredProxy(delegate, stats, time);

    return (T) Proxy.newProxyInstance(
        clazz.getClassLoader(),
        new Class[]{clazz},
        handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    final String operation = method.getName();

    final Instant t0 = time.get();
    final Object ret;
    try {
      ret = method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);

    if (delegate instanceof Storage) {
      stats.recordStorageOperation(operation, durationMillis);
    } else if (delegate instanceof DockerRunner) {
      stats.recordDockerOperation(operation, durationMillis);
    }

    return ret;
  }
}
