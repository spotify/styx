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

package com.spotify.styx.storage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IOOperationTest {

  private static final IOException CAUSE = new IOException("foobar");
  private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  @Rule public ExpectedException exception = ExpectedException.none();

  private final IOOperation<String> operation = () -> "foobar";
  private final IOOperation<String> throwingOperation = () -> { throw CAUSE; };

  @Test
  public void executeAsync() {
    assertThat(operation.executeAsync(EXECUTOR).join(), is("foobar"));
  }

  @Test
  public void executeAsyncShouldPropagateException() {
    final CompletableFuture<String> f = throwingOperation.executeAsync(EXECUTOR);
    exception.expect(CompletionException.class);
    exception.expectCause(is(CAUSE));
    f.join();
  }
}
