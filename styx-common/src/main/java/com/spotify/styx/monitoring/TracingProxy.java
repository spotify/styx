/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

import io.opencensus.common.Scope;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 * A proxy for instrumenting an instance using {@link Proxy#newProxyInstance}.
 */
public class TracingProxy implements InvocationHandler {

  private final Object delegate;
  private final String delegateName;
  private final Tracer tracer;

  private TracingProxy(Object delegate, Tracer tracer) {
    this.delegate = Objects.requireNonNull(delegate);
    this.delegateName = delegate.getClass().getSimpleName();
    this.tracer = tracer;
  }

  public static <T> T instrument(Class<T> iface, T delegate) {
    return instrument(iface, delegate, Tracing.getTracer());
  }

  @SuppressWarnings("unchecked")
  public static <T> T instrument(Class<T> iface, T delegate, Tracer tracer) {
    return (T) Proxy.newProxyInstance(
        iface.getClassLoader(),
        new Class[]{iface},
        new TracingProxy(delegate, tracer));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    final String operation = method.getName();
    final SpanBuilder spanBuilder = tracer.spanBuilder(delegateName + "." + operation);
    try (Scope ss = spanBuilder.startScopedSpan()) {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        tracer.getCurrentSpan().addAnnotation("Exception thrown");
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN);
        throw e.getTargetException();
      }
    }
  }
}
