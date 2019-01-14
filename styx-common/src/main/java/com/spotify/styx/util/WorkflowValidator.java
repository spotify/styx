/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

import static java.lang.String.format;

import com.google.common.base.Preconditions;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WorkflowValidator {

  static final long MAX_ID_LENGTH = 256;
  static final long MAX_DOCKER_ARGS_TOTAL = 1000000;
  static final long MAX_RESOURCES = 5;
  static final long MAX_RESOURCE_LENGTH = 256;
  static final long MAX_COMMIT_SHA_LENGTH = 256;
  static final long MAX_SECRET_NAME_LENGTH = 253;
  static final long MAX_SECRET_MOUNT_PATH_LENGTH = 1024;
  static final long MAX_SERVICE_ACCOUNT_LENGTH = 256;
  static final long MAX_ENV_VARS = 128;
  static final long MAX_ENV_SIZE = 16 * 1024;
  static final long MIN_RUNNING_TIMEOUT_SECONDS = 60;

  private final DockerImageValidator dockerImageValidator;
  private final Duration maxRunningTimeout;

  private WorkflowValidator(DockerImageValidator dockerImageValidator, Duration maxRunningTimeout) {
    Preconditions.checkArgument(maxRunningTimeout == null || !maxRunningTimeout.isNegative(), "Max Running timeout should be positive");
    this.dockerImageValidator = dockerImageValidator;
    this.maxRunningTimeout = maxRunningTimeout;
  }

  public static WorkflowValidator createWithRunningTimeoutLimit(DockerImageValidator dockerImageValidator, Duration maxRunningTimeout) {
    return new WorkflowValidator(dockerImageValidator, maxRunningTimeout);
  }

  public static WorkflowValidator create(DockerImageValidator dockerImageValidator) {
    return new WorkflowValidator(dockerImageValidator, null);
  }

  public List<String> validateWorkflow(Workflow workflow) {
    final WorkflowConfiguration configuration = workflow.configuration();
    return validateWorkflowConfiguration(configuration);
  }

  public List<String> validateWorkflowConfiguration(WorkflowConfiguration cfg) {
    final List<String> e = new ArrayList<>();

    // TODO: validate more of the contents

    upperLimit(e, cfg.id().length(),
        MAX_ID_LENGTH, "id too long");
    upperLimit(e, cfg.commitSha().map(String::length).orElse(0),
        MAX_COMMIT_SHA_LENGTH, "commitSha too long");
    upperLimit(e, cfg.secret().map(s -> s.name().length()).orElse(0),
        MAX_SECRET_NAME_LENGTH, "secret name too long");
    upperLimit(e, cfg.secret().map(s -> s.mountPath().length()).orElse(0),
        MAX_SECRET_MOUNT_PATH_LENGTH, "secret mount path too long");
    upperLimit(e, cfg.serviceAccount().map(String::length).orElse(0),
        MAX_SERVICE_ACCOUNT_LENGTH, "service account too long");
    upperLimit(e, cfg.resources().size(),
        MAX_RESOURCES, "too many resources");
    upperLimit(e, cfg.env().size(),
        MAX_ENV_VARS, "too many env vars");
    upperLimit(e, cfg.env().entrySet().stream()
            .mapToLong(entry -> entry.getKey().length() + entry.getValue().length()).sum(),
        MAX_ENV_SIZE, "env too big");

    cfg.dockerImage().ifPresent(image ->
        dockerImageValidator.validateImageReference(image).stream()
            .map(s -> "invalid image: " + s)
            .forEach(e::add));

    cfg.resources().stream().map(String::length).forEach(v ->
        upperLimit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    upperLimit(e, cfg.dockerArgs().map(args -> args.size() + args.stream().mapToLong(String::length).sum()),
        MAX_DOCKER_ARGS_TOTAL, "docker args is too large");

    cfg.offset().ifPresent(offset -> {
      try {
        TimeUtil.addOffset(ZonedDateTime.now(), offset);
      } catch (DateTimeParseException ex) {
        e.add(format("invalid offset: %s", ex.getMessage()));
      }
    });

    try {
      TimeUtil.cron(cfg.schedule());
    } catch (IllegalArgumentException ex) {
      e.add("invalid schedule");
    }

    lowerLimit(e, cfg.runningTimeout().map(Duration::getSeconds),
        MIN_RUNNING_TIMEOUT_SECONDS, "running timeout is too small");

    if (maxRunningTimeout != null) {
      upperLimit(e, cfg.runningTimeout(), maxRunningTimeout, "running timeout is too big");
    }

    return e;
  }

  private void upperLimit(List<String> errors, long value, long limit, String message) {
    if (value > limit) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }

  private <T extends Comparable<T>>  void upperLimit(List<String> errors, Optional<T> value, T limit, String message) {
    value.ifPresent(v -> upperLimit(errors, v, limit, message));
  }

  private <T extends Comparable<T>>  void upperLimit(List<String> errors, T value, T limit, String message) {
    if (value.compareTo(limit) > 0) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }

  private void lowerLimit(List<String> errors, Optional<Long> value, long limit, String message) {
    value.ifPresent(v -> lowerLimit(errors, v, limit, message));
  }

  private void lowerLimit(List<String> errors, long value, long limit, String message) {
    if (value < limit) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }
}
