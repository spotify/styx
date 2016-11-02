/*-
 * -\-\-
 * Spotify Styx Local Files Schedule Source
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

import com.google.auto.service.AutoService;
import com.spotify.apollo.Environment;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.schedule.ScheduleSource;
import com.spotify.styx.schedule.ScheduleSourceFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * A factory for a {@link ScheduleSource} implementation that watches a local directory
 */
@AutoService(ScheduleSourceFactory.class)
public class LocalFilesScheduleSourceFactory implements ScheduleSourceFactory {

  @Override
  public ScheduleSource create(
      Consumer<Workflow> changeListener,
      Consumer<Workflow> removeListener,
      Environment environment,
      ScheduledExecutorService exec) {
    return new LocalFileScheduleSource(
        environment.config(),
        environment.closer(),
        exec,
        changeListener,
        removeListener);
  }
}
