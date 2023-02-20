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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;

import io.grpc.Context;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GrpcContextUtilTest {

  private static final Context.Key<String> TEST_KEY = Context.key("test");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock ExecutorService executorService;

  private final ExecutorService sut = GrpcContextUtil.currentContextExecutorService(Executors.newCachedThreadPool());

  @Test
  public void shouldForward() throws InterruptedException {
    final ExecutorService mockSut = GrpcContextUtil.currentContextExecutorService(executorService);
    mockSut.shutdown();
    verify(executorService).shutdown();
    mockSut.shutdownNow();
    verify(executorService).shutdownNow();
    mockSut.isShutdown();
    verify(executorService).isShutdown();
    mockSut.isTerminated();
    verify(executorService).isTerminated();
    mockSut.awaitTermination(7, TimeUnit.HOURS);
    verify(executorService).awaitTermination(7, TimeUnit.HOURS);
  }

  @Test
  public void executeShouldPropagateContext() throws Exception {
    final CompletableFuture<String> value = new CompletableFuture<>();
    final Runnable runnable = () -> value.complete(TEST_KEY.get());
    Context.current().withValue(TEST_KEY, "foobar")
        .run(() -> sut.execute(runnable));
    assertThat(value.get(30, SECONDS), is("foobar"));
  }

  @Test
  public void submitRunnableShouldPropagateContext() throws Exception {
    final CompletableFuture<String> value = new CompletableFuture<>();
    final Runnable runnable = () -> value.complete(TEST_KEY.get());
    Context.current().withValue(TEST_KEY, "foobar")
        .run(() -> {
          try {
            sut.submit(runnable).get(30, SECONDS);
          } catch (Exception ignored) {
          }
        });
    assertThat(value.get(), is("foobar"));
  }

  @Test
  public void submitRunnableWithValueShouldPropagateContext() throws Exception {
    final CompletableFuture<String> value = new CompletableFuture<>();
    final Runnable runnable = () -> value.complete(TEST_KEY.get());
    Context.current().withValue(TEST_KEY, "foobar")
        .run(() -> {
          try {
            sut.submit(runnable, "quux").get(30, SECONDS);
          } catch (Exception ignored) {
          }
        });
    assertThat(value.get(), is("foobar"));
  }

  @Test
  public void submitCallableShouldPropagateContext() throws Exception {
    final Callable<String> callable = TEST_KEY::get;
    final Future<String> value = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> sut.submit(callable));
    assertThat(value.get(30, SECONDS), is("foobar"));
  }

  @Test
  public void invokeAllShouldPropagateContext() throws Exception {
    final Callable<String> callable = TEST_KEY::get;
    final List<Future<String>> futures = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> sut.invokeAll(List.of(callable)));
    assertThat(futures.get(0).get(30, SECONDS), is("foobar"));
  }

  @Test
  public void invokeAllWithTimeoutShouldPropagateContext() throws Exception {
    final Callable<String> callable = TEST_KEY::get;
    final List<Future<String>> futures = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> sut.invokeAll(List.of(callable), 30, SECONDS));
    assertThat(futures.get(0).get(30, SECONDS), is("foobar"));
  }

  @Test
  public void invokeAnyShouldPropagateContext() throws Exception {
    final Callable<String> callable = TEST_KEY::get;
    final String result = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> sut.invokeAny(List.of(callable)));
    assertThat(result, is("foobar"));
  }

  @Test
  public void invokeAnyWithTimeoutShouldPropagateContext() throws Exception {
    final Callable<String> callable = TEST_KEY::get;
    final String result = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> sut.invokeAny(List.of(callable), 30, SECONDS));
    assertThat(result, is("foobar"));
  }

  @Test
  public void shouldPropagateContextForCfRunAsync() throws Exception {
    final CompletableFuture<String> value = new CompletableFuture<>();
    final Runnable runnable = () -> value.complete(TEST_KEY.get());
    Context.current().withValue(TEST_KEY, "foobar")
        .run(() -> CompletableFuture.runAsync(runnable, sut).getNow(null));
    assertThat(value.get(30, SECONDS), is("foobar"));
  }

  @Test
  public void shouldPropagateContextForCfSupplyAsync() throws Exception {
    final Supplier<String> get = TEST_KEY::get;
    final CompletableFuture<String> cf = Context.current().withValue(TEST_KEY, "foobar")
        .call(() -> CompletableFuture.supplyAsync(get, sut));
    assertThat(cf.get(30, SECONDS), is("foobar"));
  }

  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(GrpcContextUtil.class), is(true));
  }
}
