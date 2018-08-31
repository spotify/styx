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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FutureUtilTest {

  @Mock private Future<String> future;

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
  public void gatherIOShouldReturnValues() throws IOException {
    final List<Future<String>> futures = ImmutableList.of(completedFuture("foo"), completedFuture("bar"));
    assertThat(FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS), contains("foo", "bar"));
  }

  @Test
  public void gatherIOShouldPropagateIOException() throws IOException {
    final IOException cause = new IOException("foo");
    final List<Future<String>> futures = ImmutableList.of(completedFuture("foo"), exceptionallyCompletedFuture(cause));
    exception.expect(is(cause));
    FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
  }

  @Test
  public void gatherIOShouldPropagateException() throws IOException {
    final Exception cause = new Exception("foo");
    final List<Future<String>> futures = ImmutableList.of(completedFuture("foo"), exceptionallyCompletedFuture(cause));
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
  }

  @Test
  public void gatherIOShouldPropagateTimeoutAsIOException()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    final TimeoutException cause = new TimeoutException();
    when(future.get(anyLong(), any())).thenThrow(cause);
    final List<Future<String>> futures = ImmutableList.of(completedFuture("foo"), future);
    exception.expect(IOException.class);
    exception.expectCause(is(cause));
    FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
  }

  @Test
  public void gatherShouldPropagateInterruptedAsIOException()
      throws InterruptedException, ExecutionException, TimeoutException {
    assumeThat("should be Java 9+", isJava9OrGreater(), is(true));
    final InterruptedException cause = new InterruptedException();
    when(future.get(anyLong(), any())).thenThrow(cause);
    final List<Future<String>> futures = ImmutableList.of(completedFuture("foo"), future);
    try {
      FutureUtil.gatherIO(futures, 30, TimeUnit.SECONDS);
      fail();
    } catch (IOException e) {
      assertThat(e.getCause(), is(cause));
    }
    assertThat(Thread.currentThread().isInterrupted(), is(true));
  }

  private static boolean isJava9OrGreater() {
    try {
      Class.forName("java.lang.ProcessHandle");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
