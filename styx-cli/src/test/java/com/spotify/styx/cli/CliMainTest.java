/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.cli;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.cli.CliMain.CliContext;
import com.spotify.styx.client.StyxClient;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CliMainTest {

  @Mock CliContext cliContext;
  @Mock StyxClient client;
  @Mock CliOutput cliOutput;

  @Before
  public void setUp() throws Exception {
    when(cliContext.createClient(any(), any())).thenReturn(client);
    when(cliContext.output(any())).thenReturn(cliOutput);
    when(cliContext.env()).thenReturn(
        ImmutableMap.of("STYX_CLI_HOST", "https://styx.foo.bar:4711"));
  }

  @Test
  public void testList() throws Exception {
    final RunStateDataPayload payload = mock(RunStateDataPayload.class);
    when(client.activeStates(Optional.empty()))
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "ls");
    verify(client).activeStates(Optional.empty());
    verify(cliOutput).printStates(payload);
  }
}
