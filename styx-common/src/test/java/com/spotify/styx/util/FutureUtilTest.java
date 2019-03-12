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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import javaslang.control.Try;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FutureUtilTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testExceptionallyCompletedFuture() throws ExecutionException, InterruptedException {
    final IOException cause = new IOException("foo");
    final CompletableFuture<Object> future = CompletableFuture.failedFuture(cause);
    exception.expect(ExecutionException.class);
    exception.expectCause(is(cause));
    future.get();
  }

  @Test
  public void gatherIOShouldReturnValues() {
    var timeout = CompletableFuture.runAsync(() -> {}, delayedExecutor(30, SECONDS));
    var futures = Map.of(
        "foo", completedFuture("foo"),
        "bar", completedFuture("bar"));
    var results = FutureUtil.gatherIO(futures, timeout);
    assertThat(results, is(Map.of(
        "foo", Try.success("foo"),
        "bar", Try.success("bar"))));
  }

  @Test
  public void gatherIOShouldPropagateException() {
    var timeout = CompletableFuture.runAsync(() -> {}, delayedExecutor(30, SECONDS));
    var cause = new Exception("foo");
    var futures = Map.of(
        "foo", completedFuture("foo"),
        "bar", CompletableFuture.<String>failedFuture(cause));
    var results = FutureUtil.gatherIO(futures, timeout);
    assertThat(results.size(), is(2));
    assertThat(results, hasEntry("foo", Try.success("foo")));
    assertThat(results.get("bar").getCause(), instanceOf(ExecutionException.class));
    assertThat(results.get("bar").getCause().getCause(), is(cause));
  }

  @Test
  public void gatherIOShouldApplyTimeouts() {
    var executor = Executors.newSingleThreadExecutor();
    // Total execution time for these futures running sequentially is 10 seconds
    var futures = IntStream.range(0, 1000).boxed()
        .collect(toMap(
            i -> i,
            i -> CompletableFuture.supplyAsync(() -> {
              sleepMillis(10);
              return i;
            }, executor)));
    var start = System.nanoTime();
    // Give enough time for ~ 20 futures to complete
    var timeout = CompletableFuture.runAsync(() -> {}, delayedExecutor(200, MILLISECONDS));
    var results = FutureUtil.gatherIO(futures, timeout);
    var end = System.nanoTime();
    var elapsed = Duration.ofNanos(end - start);
    // Verify that the timeout of 200 ms was properly applied in a non-blocking fashion by checking that the execution
    // duration is well below the expected total of 10 seconds.
    assertThat(elapsed.getSeconds(), is(lessThan(1L)));
    // Verify that the expected number of futures succeeded
    var successCount = results.values().stream().filter(Try::isSuccess).count();
    assertThat(successCount, is(both(greaterThan(10L)).and(lessThan(30L))));
  }

  private void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
