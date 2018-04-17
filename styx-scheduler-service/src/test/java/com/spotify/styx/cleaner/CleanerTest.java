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

package com.spotify.styx.cleaner;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CleanerTest {

  private CleanerOperation cleanerOperation;

  private Cleaner cleaner;

  @Before
  public void setUp() {
    cleanerOperation = spy(CleanerOperation.NOOP);
    cleaner = new Cleaner(cleanerOperation);
  }

  @Test
  public void shouldCleanup() throws CleanupException {
    cleaner.tick();
    verify(cleanerOperation).cleanup();
  }

  @Test
  public void shouldSwallowException() throws CleanupException {
    doThrow(new CleanupException("Failed to cleanup")).when(cleanerOperation).cleanup();
    cleaner.tick();
    verify(cleanerOperation).cleanup();
  }
}
