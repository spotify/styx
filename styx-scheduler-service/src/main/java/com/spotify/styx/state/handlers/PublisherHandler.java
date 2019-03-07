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

import static com.spotify.styx.state.RunState.State.RUNNING;
import static com.spotify.styx.state.RunState.State.SUBMITTED;

import com.cronutils.utils.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.Retrier;
import com.spotify.styx.util.RunnableWithException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An event consumer that integrates {@link RunState.State} values with a {@link Publisher}.
 */
public class PublisherHandler implements BiConsumer<SequenceEvent, RunState> {

  private static final Logger LOG = LoggerFactory.getLogger(PublisherHandler.class);

  private static final int MAX_RETRIES = 420;

  private static final String DEPLOYING = "deploying";
  private static final String DEPLOYED = "deployed";

  private final Retrier retrier;
  private final Publisher publisher;
  private final Stats stats;

  public PublisherHandler(Publisher publisher, Stats stats) {
    this(publisher, stats,
        Retrier.builder()
            .errorMessage("publish deploy event")
            .retryDelay(Duration.ofSeconds(1))
            .maxRetries(MAX_RETRIES)
            .build());
  }

  @VisibleForTesting
  PublisherHandler(Publisher publisher, Stats stats, Retrier retrier) {
    this.publisher = Objects.requireNonNull(publisher);
    this.stats = Objects.requireNonNull(stats);
    this.retrier = Objects.requireNonNull(retrier);
  }

  @Override
  public void accept(SequenceEvent sequenceEvent, RunState state) {
    final WorkflowInstance workflowInstance = state.workflowInstance();
    switch (state.state()) {
      case SUBMITTED:
        try {
          Preconditions.checkArgument(state.data().executionDescription().isPresent());
          final ExecutionDescription executionDescription = state.data().executionDescription().get();

          retrier.runWithRetries(
              meteredPublishing(() -> publisher.deploying(workflowInstance, executionDescription),
                  stats, DEPLOYING, SUBMITTED.name()));
        } catch (Exception e) {
          stats.recordPublishingError(DEPLOYING, SUBMITTED.name());
          LOG.error("Failed to publish event for {} state", SUBMITTED.name(), e);
        }
        break;

      case RUNNING:
        final String type = "deployed";
        try {
          Preconditions.checkArgument(state.data().executionDescription().isPresent());
          final ExecutionDescription executionDescription = state.data().executionDescription().get();

          retrier.runWithRetries(
              meteredPublishing(() -> publisher.deployed(workflowInstance, executionDescription),
                  stats, type, RUNNING.name()));
        } catch (Exception e) {
          stats.recordPublishingError(DEPLOYED, RUNNING.name());
          LOG.error("Failed to publish event for {} state", RUNNING.name(), e);
        }
        break;

      default:
        // do nothing
    }
  }

  private RunnableWithException<Exception> meteredPublishing(RunnableWithException<Exception> runnable,
                                                             Stats stats, String type, String state) {
    return () -> {
      try {
        runnable.run();
        stats.recordPublishing(type, state);
      } catch (Exception e) {
        stats.recordPublishingError(type, state);
        LOG.warn("Failed to publish event for {} state", state, e);

        throw e;
      }
    };
  }
}
