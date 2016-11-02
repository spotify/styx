/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
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

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.util.Objects;

public class MeteredDockerRunner extends MeteredBase implements DockerRunner {

  private final DockerRunner delegate;

  public MeteredDockerRunner(DockerRunner delegate, Stats stats, Time time) {
    super(stats, time);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    return timedDocker("start", () -> delegate.start(workflowInstance, runSpec));
  }


  @Override
  public void cleanup(String executionId) {
    timedDocker("cleanup", () -> delegate.cleanup(executionId));
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
