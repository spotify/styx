/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import com.spotify.styx.state.RunState;
import com.spotify.styx.testdata.TestData;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NoopFlyteRunnerTest {

  private static final FlyteExecutionId FLYTE_EXECUTION_ID =
      FlyteExecutionId.create("flyte-test", "testing", "exec-name");

  private final NoopFlyteRunner runner = new NoopFlyteRunner();
  @Mock private RunState runState;

  @Test
  public void testRunnerInNotEnabled() {
    assertFalse(runner.isEnabled());
  }

  @Test
  public void testCreateExecutionThrowsException() {
    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> runner.createExecution(runState, "name", TestData.FLYTE_EXEC_CONF, Map.of())
    );
  }

  @Test
  public void testTerminateExecutionThrowsException() {
    assertThrows(
        IllegalStateException.class,
        () -> runner.terminateExecution(null, FLYTE_EXECUTION_ID)
    );
  }

  @Test
  public void testPollingThrowsException() {
    assertThrows(
        IllegalStateException.class,
        () -> runner.poll(FLYTE_EXECUTION_ID, null)
    );
  }

  @Test
  public void testCloseDoesNotThrowsExeption() {
    runner.close();
  }
}
