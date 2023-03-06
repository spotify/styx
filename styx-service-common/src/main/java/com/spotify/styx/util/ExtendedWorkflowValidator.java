/*
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2019 Spotify AB
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

package com.spotify.styx.util;

import static com.spotify.styx.util.ConfigUtil.get;
import static com.spotify.styx.util.WorkflowValidator.upperLimit;

import com.google.common.base.Preconditions;
import com.spotify.apollo.Environment;
import com.spotify.styx.model.Workflow;
import com.typesafe.config.Config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Extended workflow validation can only be done on service side.
 */
public class ExtendedWorkflowValidator implements WorkflowValidator {

  private final WorkflowValidator delegate;
  private final Duration maxRunningTimeout;

  static final String STYX_RUNNING_STATE_MAX_TTL_CONFIG = "styx.stale-state-ttls.running_max";
  private static final Duration DEFAULT_STYX_RUNNING_STATE_TTL = Duration.ofHours(24);

  public ExtendedWorkflowValidator(WorkflowValidator delegate,
                                   Config config) {
    final Duration maxRunningTimeout = get(config, config::getString, STYX_RUNNING_STATE_MAX_TTL_CONFIG)
            .map(Duration::parse)
            .orElse(DEFAULT_STYX_RUNNING_STATE_TTL);

    Preconditions.checkArgument(maxRunningTimeout != null && !maxRunningTimeout.isNegative(),
        "Max Running timeout should be positive");
    this.delegate = Objects.requireNonNull(delegate);
    this.maxRunningTimeout = maxRunningTimeout;
  }

  @Override
  public List<String> validateWorkflow(Workflow workflow) {
    var e = new ArrayList<>(delegate.validateWorkflow(workflow));

    var cfg = workflow.configuration();

    cfg.runningTimeout().ifPresent(timeout ->
        upperLimit(e, timeout, maxRunningTimeout, "running timeout is too big"));

    return e;
  }
}
