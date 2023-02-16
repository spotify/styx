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

import static com.spotify.styx.util.CloserUtil.DEFAULT_TIMEOUT;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Closer;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CloserUtilTest {

  @Mock private ExecutorService executorService;
  @Mock private Runnable runnable;

  private final Closer closer = Closer.create();

  @Test
  public void shouldCloseRegisteredExecutorService() throws IOException, InterruptedException {
    final ExecutorService registeredExecutorService = CloserUtil.register(closer, executorService, "foobar");
    assertThat(registeredExecutorService, is(executorService));
    when(executorService.shutdownNow()).thenReturn(List.of(runnable));
    closer.close();
    verifyShutdown(executorService);
  }

  @Test
  public void closeableShouldCloseExecutorService() throws IOException, InterruptedException {
    final Closeable closeable = CloserUtil.closeable(executorService, "foobar");
    when(executorService.shutdownNow()).thenReturn(List.of(runnable));
    closeable.close();
    verifyShutdown(executorService);
  }

  @Test
  public void shouldHandleInterruption() throws IOException, InterruptedException {
    // Interruptions break code coverage in Java 8
    // https://github.com/jacoco/eclemma/issues/64
    // https://bugs.openjdk.java.net/browse/JDK-8154017
    assumeThat("should be Java 9+", isJava9OrGreater(), is(true));
    final Closeable closeable = CloserUtil.closeable(executorService, "foobar");
    when(executorService.shutdownNow()).thenReturn(List.of(runnable));
    doThrow(new InterruptedException()).when(executorService).awaitTermination(anyLong(), any());
    closeable.close();
    assertThat(Thread.currentThread().isInterrupted(), is(true));
    verifyShutdown(executorService);
  }

  private static void verifyShutdown(ExecutorService executorService) throws InterruptedException {
    verify(executorService).shutdown();
    verify(executorService).awaitTermination(DEFAULT_TIMEOUT.toNanos(), NANOSECONDS);
    verify(executorService).shutdownNow();
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
