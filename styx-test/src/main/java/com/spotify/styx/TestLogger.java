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

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunListener.ThreadSafe
public class TestLogger extends RunListener {

  private final Logger log;

  TestLogger() {
    this(LoggerFactory.getLogger(TestLogger.class));
  }

  TestLogger(Logger log) {
    this.log = log;
  }

  @Override
  public void testStarted(Description description) {
    log.info("Test started: {}", description);
  }

  @Override
  public void testFinished(Description description) {
    log.info("Test finished: {}", description);
  }

  @Override
  public void testFailure(Failure failure) {
    log.info("Test failed: {}", failure, failure.getException());
  }

  @Override
  public void testAssumptionFailure(Failure failure) {
    log.info("Test assumption failed: {}", failure);
  }

  @Override
  public void testIgnored(Description description) {
    log.info("Test ignored: {}", description);
  }
}
