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

import static com.spotify.styx.util.FutureUtil.exceptionallyCompletedFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
    final CompletableFuture<Object> future = exceptionallyCompletedFuture(cause);
    exception.expect(ExecutionException.class);
    exception.expectCause(is(cause));
    future.get();
  }

  @Test
  public void gatherIOShouldReturnValues() {
    var futures = Map.of(
        "foo", completedFuture("foo"),
        "bar", completedFuture("bar"));
    var results = FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
    assertThat(results, is(Map.of(
        "foo", Try.success("foo"),
        "bar", Try.success("bar"))));
  }

  @Test
  public void gatherIOShouldPropagateException() {
    var cause = new Exception("foo");
    var futures = Map.of(
        "foo", completedFuture("foo"),
        "bar", CompletableFuture.<String>failedFuture(cause));
    var results = FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
    assertThat(results.size(), is(2));
    assertThat(results, hasEntry("foo", Try.success("foo")));
    assertThat(results.get("bar").getCause(), instanceOf(ExecutionException.class));
    assertThat(results.get("bar").getCause().getCause(), is(cause));
  }
}
