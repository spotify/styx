/*-
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

package com.spotify.styx.state.handlers;

import com.google.common.base.Preconditions;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.Retrier;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} that integrates {@link RunState.State} values with a {@link Publisher}.
 */
public class PublisherHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PublisherHandler.class);

  private static final int MAX_RETRIES = 420;
  private static final Retrier RETRIER = Retrier.builder()
      .errorMessage("publish deploy event")
      .retryDelay(Duration.ofSeconds(1))
      .maxRetries(MAX_RETRIES)
      .build();

  private final Publisher publisher;

  public PublisherHandler(Publisher publisher) {
    this.publisher = Objects.requireNonNull(publisher);
  }

  @Override
  public void transitionInto(RunState state) {
    final WorkflowInstance workflowInstance = state.workflowInstance();
    switch (state.state()) {
      case SUBMITTED:
        try {
          Preconditions.checkArgument(state.data().executionDescription().isPresent());
          final ExecutionDescription executionDescription = state.data().executionDescription().get();
          RETRIER.runWithRetries(() -> publisher.deploying(workflowInstance, executionDescription));
        } catch (Exception e) {
          LOG.error("Failed to publish event for PREPARE state", e);
        }
        break;

      case RUNNING:
        try {
          Preconditions.checkArgument(state.data().executionDescription().isPresent());
          final ExecutionDescription executionDescription = state.data().executionDescription().get();
          RETRIER.runWithRetries(() -> publisher.deployed(workflowInstance, executionDescription));
        } catch (Exception e) {
          LOG.error("Failed to publish event for RUNNING state", e);
        }
        break;

      default:
        // do nothing
    }
  }
}
