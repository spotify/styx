/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.monitoring;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredStorageProxyTest {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Mock private Stats stats;
  @Mock private Storage storage;

  private Instant now = Instant.now();
  private Instant later = now.plusMillis(123);
  private List<Instant> times = Arrays.asList(now, later);
  private int pos = 0;
  private Time time = () -> times.get(pos++);

  private Storage proxy;

  @Before
  public void setUp() {
    proxy = MeteredStorageProxy.instrument(storage, stats, time);
  }

  @Test
  public void instrumentStorageMethod() throws Exception {
    proxy.resource("foobar");

    verify(storage).resource("foobar");
    verify(stats).recordStorageOperation("resource", 123, "success");
  }

  @Test
  public void surfaceExceptions() throws Exception {
    doThrow(new RuntimeException("with message")).when(storage).resource("foobar");

    expect.expect(RuntimeException.class);
    expect.expectMessage("with message");

    proxy.resource("foobar");
  }
}
