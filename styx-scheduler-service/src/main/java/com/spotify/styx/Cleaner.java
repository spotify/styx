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

package com.spotify.styx;

import com.spotify.styx.docker.DockerRunner;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Cleaner {

  private static final Logger logger = LoggerFactory.getLogger(Cleaner.class);

  private static final Tracer tracer = Tracing.getTracer();

  private final DockerRunner dockerRunner;

  Cleaner(DockerRunner dockerRunner) {
    this.dockerRunner = Objects.requireNonNull(dockerRunner);
  }

  void tick() {
    try (Scope ss = tracer.spanBuilder("Styx.Cleaner.tick").startScopedSpan()) {
      tick0();
    }
  }

  void tick0() {
    try {
      dockerRunner.cleanup();
    } catch (IOException e) {
      logger.warn("Docker runner cleanup failed", e);
    }
  }
}
