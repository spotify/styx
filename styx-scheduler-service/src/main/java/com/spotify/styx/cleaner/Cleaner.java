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

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cleaner {

  private static final Logger logger = LoggerFactory.getLogger(Cleaner.class);

  private final CleanerOperation cleanerOperation;

  public Cleaner(CleanerOperation cleanerOperation) {
    this.cleanerOperation = Objects.requireNonNull(cleanerOperation);
  }

  public void tick() {
    try {
      cleanerOperation.cleanup();
    } catch (CleanupException e) {
      logger.warn("Cleanup operation failed", e);
    }
  }
}
