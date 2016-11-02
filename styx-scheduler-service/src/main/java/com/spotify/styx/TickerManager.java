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
package com.spotify.styx;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Maps;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Manages {@link com.spotify.styx.model.DataEndpoint} and {@link TickTock} instances that
 * generate time based events.
 */
class TickerManager {

  private final Function<Workflow, TickTock> tickTockFactory;

  private final ConcurrentMap<WorkflowId, TickTock> tickers = Maps.newConcurrentMap();

  TickerManager(Function<Workflow, TickTock> tickTockFactory) {
    this.tickTockFactory = requireNonNull(tickTockFactory);
  }

  void removeWorkflow(Workflow workflow) {
    tickers.computeIfPresent(workflow.id(), (ignore, existing) -> {
      close(existing);
      return null;
    });
  }

  void updateWorkflow(Workflow workflow) {
    tickers.compute(workflow.id(), (ignoredId, existing) -> {
      if (existing != null) {
        if (existing.workflow().equals(workflow)) {
          return existing;
        }

        close(existing);
      }

      final TickTock tickTock = tickTockFactory.apply(workflow);
      tickTock.start();
      return tickTock;
    });
  }

  private void close(TickTock existing) {
    try {
      existing.close();
    } catch (IOException ignore) {
    }
  }
}
