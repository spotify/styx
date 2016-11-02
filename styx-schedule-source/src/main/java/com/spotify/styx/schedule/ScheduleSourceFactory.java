/*-
 * -\-\-
 * Spotify Styx Schedule Source API
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

package com.spotify.styx.schedule;

import com.spotify.apollo.Environment;
import com.spotify.styx.model.Workflow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * A factory for creating {@link ScheduleSource}. A schedule source is responsible for making
 * read-only access to a set of {@link Workflow}s available to styx from another data source.
 * When ScheduleSource.start() is called, changeListener gets called for each available Workflow.
 * After the initial bootstrap, making all data available, changeListener gets called for updated
 * or added Workflows, and removeListener gets called whenever a Workflow is removed.
 *
 * This interface is intended to be used as an Service Provider Interface. Styx will discover
 * implementations on the classpath using {@link java.util.ServiceLoader}.
 */
public interface ScheduleSourceFactory {

  /**
   * Crate a new {@link ScheduleSource}.
   *
   * @param changeListener  A callback that will be invoked whenever a workflow in created/changed.
   *                        On startup, changeListener gets called for all available workflows to
   *                        bootstrap the system.
   * @param removeListener  A callback that will be invoked whenever a workflow is removed
   * @param environment     An Apollo environment that might be used
   * @param exec            An executor that might be used
   * @return A schedule source that is ready to be started
   */
  ScheduleSource create(
      Consumer<Workflow> changeListener,
      Consumer<Workflow> removeListener,
      Environment environment,
      ScheduledExecutorService exec);
}
