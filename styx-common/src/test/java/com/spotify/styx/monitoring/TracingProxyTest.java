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

import static io.opencensus.trace.Status.UNKNOWN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.util.MockSpan;
import com.spotify.styx.util.Time;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Tracer;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TracingProxyTest {

  @Mock Tracer tracer;
  @Mock SpanBuilder spanBuilder;
  @Mock Time time;

  @Before
  public void setUp() throws Exception {
    when(time.get()).then(a -> Instant.now());
  }

  @Test
  public void testTracingSuccess() {
    final MockSpan span = new MockSpan();

    final Foo delegate = new FooImpl();
    final Foo proxy = TracingProxy.instrument(Foo.class, delegate, tracer);

    when(tracer.spanBuilder("FooImpl.bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    proxy.bar();

    verify(tracer).spanBuilder("FooImpl.bar");
    verify(spanBuilder).startSpan();
    assertThat(span.ended, is(true));
    assertThat(span.status, is(nullValue()));
    assertThat(span.annotations, is(empty()));
  }

  @Test
  public void testTracingException() {
    final MockSpan span = new MockSpan();

    final Foo delegate = new ThrowingFooImpl();

    final Foo proxy = TracingProxy.instrument(Foo.class, delegate, tracer);

    when(tracer.spanBuilder("ThrowingFooImpl.bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    try {
      proxy.bar();
      fail();
    } catch (Exception ignore) {
    }

    verify(tracer).spanBuilder("ThrowingFooImpl.bar");
    verify(spanBuilder).startSpan();
    assertThat(span.ended, is(true));
    assertThat(span.status, is(UNKNOWN));
    assertThat(span.annotations, contains(Annotation.fromDescription("Exception thrown")));
  }

  interface Foo {

    void bar();
  }

  class FooImpl implements Foo {

    @Override
    public void bar() {
      // nop
    }
  }

  class ThrowingFooImpl implements Foo {

    @Override
    public void bar() {
      throw new RuntimeException("bar!");
    }
  }
}
