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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import javaslang.control.Try;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CachedSupplierTest {

  private volatile Instant instant = Instant.parse("2016-10-17T15:00:00Z");
  private volatile int x = 42;

  @Mock private Try.CheckedSupplier<Integer> delegate;

  private Supplier<Integer> sut;
  @Mock private Time time;

  @Before
  public void setUp() throws Throwable {
    when(delegate.get()).then(a -> x);
    when(time.nanoTime()).then(a -> MILLISECONDS.toNanos(instant.toEpochMilli()));
    sut = new CachedSupplier<>(delegate, time, Duration.ofSeconds(10));
  }

  @Test
  public void testCachesCalls() throws Throwable {
    int a = sut.get();
    x = 100;
    int b = sut.get();

    verify(delegate, times(1)).get();

    assertThat(a, is(42));
    assertThat(b, is(42));
  }

  @Test
  public void testCacheTimesOut() throws Throwable {
    int a = sut.get();
    x = 100;
    instant = Instant.parse("2016-10-17T15:00:11Z");
    int b = sut.get();

    verify(delegate, times(2)).get();
    assertThat(a, is(42));
    assertThat(b, is(100));
  }

  @Test
  public void testCacheDoesNotTimeOutWithinTimeout() throws Throwable {
    int a = sut.get();
    x = 100;
    instant = Instant.parse("2016-10-17T15:00:09Z");
    int b = sut.get();

    verify(delegate, times(1)).get();
    assertThat(a, is(42));
    assertThat(b, is(42));
  }

  @Test
  public void shouldOnlyComputeOnceForConcurrentCalls() throws Throwable {
    var executor = Executors.newCachedThreadPool();
    var queue = new LinkedBlockingQueue<Integer>();
    when(delegate.get()).then(a -> queue.take());

    var f1 = CompletableFuture.supplyAsync(sut, executor);
    var f2 = CompletableFuture.supplyAsync(sut, executor);

    // Wait for first thread to call delegate
    verify(delegate, timeout(5000).times(1)).get();

    // Wait for second thread to reach lock
    Thread.sleep(500);
    verifyNoMoreInteractions(delegate);

    queue.put(17);
    queue.put(4711);

    assertThat(f1.join(), is(17));
    assertThat(f2.join(), is(17));

    verifyNoMoreInteractions(delegate);
  }
}
