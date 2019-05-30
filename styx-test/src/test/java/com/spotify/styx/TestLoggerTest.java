/*-
 * -\-\-
 * Spotify Styx Testing Utilities
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;

public class TestLoggerTest {

  private final Logger log = mock(Logger.class);

  private final TestLogger sut = new TestLogger(log);

  private final Description description = Description.createTestDescription("Foo", "bar");
  private final Throwable cause = new Exception("Failed!");
  private final Failure failure = new Failure(description, cause);

  @Test
  public void testDefaultConstructor() {
    var testLogger = new TestLogger();
    Assert.assertThat(testLogger, is(notNullValue()));
  }

  @Test
  public void testStarted() {
    sut.testStarted(description);
    verify(log).info("Test started: {}", description);
  }

  @Test
  public void testFinished() {
    sut.testFinished(description);
    verify(log).info("Test finished: {}", description);
  }

  @Test
  public void testFailure() {
    sut.testFailure(failure);
    verify(log).info("Test failed: {}", failure, cause);
  }

  @Test
  public void testAssumptionFailure() {
    sut.testAssumptionFailure(failure);
    verify(log).info("Test assumption failed: {}", failure);
  }

  @Test
  public void testIgnored() {
    sut.testIgnored(description);
    verify(log).info("Test ignored: {}", description);
  }
}