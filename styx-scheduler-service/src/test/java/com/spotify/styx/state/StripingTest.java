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

package com.spotify.styx.state;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doCallRealMethod;

import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedRunnable;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.MDC;

@RunWith(MockitoJUnitRunner.class)
public class StripingTest {

  @Spy StripedExecutorService stripedExecutorService = new StripedExecutorService();
  @Captor ArgumentCaptor<Runnable> runnableArgumentCaptor;

  @Before
  public void setUp() throws Exception {
    MDC.clear();
  }

  @After
  public void tearDown() throws Exception {
    stripedExecutorService.shutdownNow();
    MDC.clear();
  }

  @Test
  public void supplyAsyncStriped() throws ExecutionException, InterruptedException {
    doCallRealMethod().when(stripedExecutorService).execute(runnableArgumentCaptor.capture());

    MDC.put("foo", "bar");

    Striping.supplyAsyncStriped(() -> {
      assertThat(MDC.get("foo"), is("bar"));
      return "foo";
    }, "baz", stripedExecutorService).get();

    final Runnable runnable = runnableArgumentCaptor.getValue();
    assertThat(runnable, instanceOf(StripedRunnable.class));
    assertThat(((StripedRunnable) runnable).getStripe(), is("baz"));
  }
}
