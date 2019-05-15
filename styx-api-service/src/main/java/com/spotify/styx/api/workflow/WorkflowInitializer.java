/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.api.workflow;

import static com.spotify.styx.util.TimeUtil.nextInstant;

import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowInitializer.class);

  private final Storage storage;
  private final Time time;

  public WorkflowInitializer(Storage storage, Time time) {
    this.storage = Objects.requireNonNull(storage);
    this.time = Objects.requireNonNull(time);
  }

  public Optional<Workflow> store(Workflow workflow, Consumer<Optional<Workflow>> guard)
      throws WorkflowInitializationException {
    try {
      return storage.runInTransactionWithRetries(tx -> store(tx, workflow, guard));
    } catch (IOException e) {
      LOG.warn("failed to write workflow {} to storage", workflow.id(), e);
      throw new RuntimeException(e);
    }
  }

  private Optional<Workflow> store(StorageTransaction tx, Workflow workflow,
                                   Consumer<Optional<Workflow>> guard)
      throws WorkflowInitializationException, IOException {
    final Optional<Workflow> previous;
    try {
      previous = tx.workflow(workflow.id());
    } catch (IOException e) {
      LOG.warn("failed to read workflow {} from storage", workflow.id(), e);
      throw new RuntimeException(e);
    }

    guard.accept(previous);

    final Optional<TriggerInstantSpec> nextSpec;

    // either the workflow is completely new, or the schedule/offset has changed
    final Schedule newSchedule = workflow.configuration().schedule();
    final Optional<String> newOffset = workflow.configuration().offset();
    if (!previous.isPresent()
        || !previous.get().configuration().schedule().equals(newSchedule)
        || !previous.get().configuration().offset().equals(newOffset)) {
      try {
        nextSpec = Optional.of(initializeNaturalTrigger(workflow));
      } catch (Exception e) {
        LOG.info("could not compute next natural trigger for workflow {}", workflow, e);
        throw new WorkflowInitializationException(e);
      }
    } else {
      nextSpec = Optional.empty();
    }

    if (nextSpec.isPresent()) {
      tx.storeWorkflowWithNextNaturalTrigger(workflow, nextSpec.get());
    } else {
      tx.store(workflow);
    }

    return previous;
  }

  private TriggerInstantSpec initializeNaturalTrigger(Workflow workflow) {
    final Instant now = time.get();
    final Instant offsetNow = workflow.configuration().subtractOffset(now);
    final Schedule schedule = workflow.configuration().schedule();
    final Instant nextTrigger = nextInstant(offsetNow, schedule);
    final Instant nextWithOffset = workflow.configuration().addOffset(nextTrigger);
    return TriggerInstantSpec.create(nextTrigger, nextWithOffset);
  }
}
