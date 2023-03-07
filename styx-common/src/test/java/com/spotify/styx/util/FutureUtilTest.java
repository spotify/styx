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

package com.spotify.styx.util;

import static com.spotify.styx.util.FutureUtil.gatherIO;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FutureUtilTest {

  private final CompletableFuture<Void> NO_TIMEOUT = new CompletableFuture<>();
  private final CompletableFuture<Void> TIMED_OUT = CompletableFuture.completedFuture(null);

  private CompletableFuture<String> future = new CompletableFuture<>();

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Test
  public void gatherIOShouldReturnValues() throws IOException {
    var futures = List.of(completedFuture("foo"), completedFuture("bar"));
    assertThat(FutureUtil.gatherIO(futures, NO_TIMEOUT), contains("foo", "bar"));
  }

  @Test
  public void gatherIOShouldPropagateIOException() throws IOException {
    var cause = new IOException("foo");
    var futures = List.of(completedFuture("foo"), failedFuture(cause));
    exception.expect(is(cause));
    FutureUtil.gatherIO(futures, NO_TIMEOUT);
  }

  @Test
  public void gatherIOShouldPropagateException() throws IOException {
    var cause = new Exception("foo");
    var futures = List.of(completedFuture("foo"), failedFuture(cause));
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    FutureUtil.gatherIO(futures, NO_TIMEOUT);
  }

  @Test
  public void gatherIOShouldPropagateTimeoutAsIOException() throws IOException {
    var futures = List.of(completedFuture("foo"), future);
    exception.expect(IOException.class);
    exception.expectCause(instanceOf(TimeoutException.class));
    FutureUtil.gatherIO(futures, TIMED_OUT);
  }

  @Test
  public void gatherShouldPropagateInterruptedAsIOException() throws Exception {
    var futures = List.of(completedFuture("foo"), future);
    Thread.currentThread().interrupt();
    try {
      FutureUtil.gatherIO(futures, NO_TIMEOUT);
      fail();
    } catch (IOException e) {
      assertThat(e.getCause(), instanceOf(InterruptedException.class));
    }
    // verify that gatherIO interrupts current thread after catching InterruptedException
    assertThat(Thread.currentThread().isInterrupted(), is(true));
    exception.expect(InterruptedException.class);
    Thread.sleep(1000);
  }

  @Test
  public void gatherIOShouldApplyTimeouts() throws IOException {
    var executor = Executors.newSingleThreadExecutor();
    // Total execution time for these futures running sequentially is 10 seconds
    var futures = IntStream.range(0, 1000).boxed()
        .map(i -> CompletableFuture.supplyAsync(() -> {
          sleepMillis(10);
          return i;
        }, executor))
        .collect(toList());
    var start = System.nanoTime();
    // Give enough time for ~ 20 futures to complete
    var timeout = CompletableFuture.runAsync(() -> {}, delayedExecutor(200, MILLISECONDS));
    try {
      gatherIO(futures, timeout);
      fail();
    } catch (IOException e) {
      assertThat(e.getCause(), instanceOf(TimeoutException.class));
    }
    var end = System.nanoTime();
    var elapsed = Duration.ofNanos(end - start);
    // Verify that the timeout of 200 ms was properly applied in a non-blocking fashion by checking that the execution
    // duration is well below the expected total of 10 seconds.
    assertThat(elapsed.getSeconds(), is(lessThan(1L)));
  }

  private void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
