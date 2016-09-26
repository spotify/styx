/*
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

/**
 * Instances of ScheduleSource makes information about Workflows available from a secondary
 * system. The instance communicates information about those workflows back to styx using
 * the changeListener and removeListener parameters to ScheduleSourceFactory.create().
 */
public interface ScheduleSource {

  /**
   * Starts the flow of Workflow change and remove information by calling changeListener
   * for all available Workflows in the external storage that this instance represents,
   * and calls changeListener and removeListener whenever Workflows are changed or removed.
   */
  void start();
}
