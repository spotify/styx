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

import static com.spotify.styx.util.CachedSupplier.DEFAULT_TIMEOUT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.mockito.Mockito.lenient;
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
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CachedSupplierTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final int INITIAL_VALUE = 42;
  private static final int NEW_VALUE = 100;

  private volatile Instant now = Instant.parse("2016-10-17T15:00:00Z");

  @Mock private Try.CheckedSupplier<Integer> delegate;

  private Supplier<Integer> sut;
  @Mock private Time time;

  @Before
  public void setUp() throws Throwable {
    lenient().when(delegate.get()).thenReturn(INITIAL_VALUE);
    when(time.nanoTime()).then(a -> MILLISECONDS.toNanos(now.toEpochMilli()));
    sut = new CachedSupplier<>(delegate, time, TIMEOUT);
  }

  @Test
  public void testCachesCalls() throws Throwable {
    // Initial load
    var a = sut.get();
    assertThat(a, is(INITIAL_VALUE));
    verify(delegate, times(1)).get();

    // Make underlying supplier return new value
    lenient().when(delegate.get()).thenReturn(NEW_VALUE);

    // Should still get initial value here
    var b = sut.get();
    assertThat(b, is(INITIAL_VALUE));

    // Verify there were no additional calls to the underlying supplier
    verifyNoMoreInteractions(delegate);
  }

  @Test
  public void testReturnsOldValueIfDelegateFails() throws Throwable {
    // Initial load
    var a = sut.get();
    assertThat(a, is(INITIAL_VALUE));
    verify(delegate, times(1)).get();

    // Make the supplier fail and progress time enough to trigger refresh
    when(delegate.get()).thenThrow(new RuntimeException("Fail!"));
    now = now.plus(TIMEOUT).plusSeconds(1);

    // Trigger refresh
    var b = sut.get();
    assertThat(b, is(INITIAL_VALUE));

    // Ensure that the old value is still returned (never observe another value)
    exception.expect(ConditionTimeoutException.class);
    await().atMost(5, SECONDS).until(() -> sut.get() != INITIAL_VALUE);
  }

  @Test
  public void testCacheRefreshes() throws Throwable {
    // Initial load
    var a = sut.get();
    assertThat(a, is(INITIAL_VALUE));
    verify(delegate, times(1)).get();

    // Make supplier return new value and progress time enough to trigger refresh
    when(delegate.get()).thenReturn(NEW_VALUE);
    now = now.plus(TIMEOUT).plusSeconds(1);

    // Eventually observe new value
    await().until(() -> sut.get() == NEW_VALUE);
  }

  @Test
  public void testCacheDoesNotRefreshBeforeTimeout() throws Throwable {
    // Initial load
    int a = sut.get();
    assertThat(a, is(INITIAL_VALUE));
    verify(delegate, times(1)).get();

    // Make supplier return new value and progress time, but not enough to trigger refresh
    lenient().when(delegate.get()).thenReturn(NEW_VALUE);
    now = now.plus(TIMEOUT).minusSeconds(1);

    // Should return initial value and not trigger refresh
    int b = sut.get();
    assertThat(b, is(INITIAL_VALUE));

    // Ensure that there is no call to refresh
    Thread.sleep(5000);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  public void shouldOnlyComputeOnceForConcurrentCalls() throws Throwable {
    var executor = Executors.newCachedThreadPool();
    var queue = new LinkedBlockingQueue<Integer>();
    when(delegate.get()).then(a -> queue.take());

    var f1 = CompletableFuture.supplyAsync(sut, executor);
    var f2 = CompletableFuture.supplyAsync(sut, executor);

    // Wait for first call to the underlying supplier
    verify(delegate, timeout(5000).times(1)).get();

    queue.put(1);
    queue.put(2);

    assertThat(f1.join(), is(1));
    assertThat(f2.join(), is(1));

    // Ensure that there was only one call to the underlying supplier
    Thread.sleep(5000);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  public void defaultTimeout() throws Throwable {
    sut = new CachedSupplier<>(delegate, time);

    // Initial load
    var a = sut.get();
    assertThat(a, is(INITIAL_VALUE));
    verify(delegate, times(1)).get();

    // Make supplier return new value and progress time enough to trigger refresh
    when(delegate.get()).thenReturn(NEW_VALUE);
    now = now.plus(DEFAULT_TIMEOUT).plusSeconds(1);

    // Eventually observe new value
    await().until(() -> sut.get() == NEW_VALUE);
  }

  @Test
  public void refreshesAsynchronously() throws Throwable {
    // Initial load
    var initialValue = sut.get();
    assertThat(initialValue, is(INITIAL_VALUE));
    verify(delegate).get();

    // Set up reload to block
    var reloadFuture = new CompletableFuture<Integer>();
    when(delegate.get()).then(a -> reloadFuture.join());

    // Progress time enough to exceed timeout
    now = now.plus(TIMEOUT).plusSeconds(1);

    // Trigger refresh - should return old value and not block
    var triggerValue = sut.get();
    assertThat(triggerValue, is(INITIAL_VALUE));

    // Wait for refresh
    await().until(() -> Try.of(() -> verify(delegate, times(2)).get()).isSuccess());

    // Complete reload
    reloadFuture.complete(NEW_VALUE);

    // Eventually observe new value
    await().until(() -> sut.get() == NEW_VALUE);
  }
}
