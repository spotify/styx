/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class MeteredProxyTest {

  Instant now = Instant.now();
  Instant later = now.plusMillis(123);
  List<Instant> times = Arrays.asList(now, later);
  int pos = 0;

  Stats stats = mock(Stats.class);
  Time time = () -> times.get(pos++);

  @Test
  public void instrumentStorageMethod() throws Exception {
    Storage mock = mock(Storage.class);
    Storage proxy = MeteredProxy.instrument(Storage.class, mock, stats, time);

    proxy.resource("foobar");

    verify(mock).resource("foobar");
    verify(stats).storageOperation("resource", 123);
  }

  @Test
  public void instrumentDockerMethod() throws Exception {
    DockerRunner mock = mock(DockerRunner.class);
    DockerRunner proxy = MeteredProxy.instrument(DockerRunner.class, mock, stats, time);

    proxy.cleanup("barbaz");

    verify(mock).cleanup("barbaz");
    verify(stats).dockerOperation("cleanup", 123);
  }
}
