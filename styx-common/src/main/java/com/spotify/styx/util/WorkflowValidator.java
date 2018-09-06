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

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
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

  private final DockerImageValidator dockerImageValidator;

  public WorkflowValidator(DockerImageValidator dockerImageValidator) {
    this.dockerImageValidator = dockerImageValidator;
  }

  public List<String> validateWorkflow(Workflow workflow) {
    final WorkflowConfiguration configuration = workflow.configuration();
    return validateWorkflowConfiguration(configuration);
  }

  public List<String> validateWorkflowConfiguration(WorkflowConfiguration cfg) {
    final List<String> e = new ArrayList<>();

    // TODO: validate more of the contents

    limit(e, cfg.id().length(),
        MAX_ID_LENGTH, "id too long");
    limit(e, cfg.commitSha().map(String::length).orElse(0),
        MAX_COMMIT_SHA_LENGTH, "commitSha too long");
    limit(e, cfg.secret().map(s -> s.name().length()).orElse(0),
        MAX_SECRET_NAME_LENGTH, "secret name too long");
    limit(e, cfg.secret().map(s -> s.mountPath().length()).orElse(0),
        MAX_SECRET_MOUNT_PATH_LENGTH, "secret mount path too long");
    limit(e, cfg.serviceAccount().map(String::length).orElse(0),
        MAX_SERVICE_ACCOUNT_LENGTH, "service account too long");
    limit(e, cfg.resources().size(),
        MAX_RESOURCES, "too many resources");
    limit(e, cfg.env().size(),
        MAX_ENV_VARS, "too many env vars");
    limit(e, cfg.env().entrySet().stream()
            .mapToLong(entry -> entry.getKey().length() + entry.getValue().length()).sum(),
        MAX_ENV_SIZE, "env too big");

    cfg.dockerImage().ifPresent(image ->
        dockerImageValidator.validateImageReference(image).stream()
            .map(s -> "invalid image: " + s)
            .forEach(e::add));

    cfg.resources().stream().map(String::length).forEach(v ->
        limit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    limit(e, cfg.dockerArgs().map(args -> args.size() + args.stream().mapToLong(String::length).sum()),
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

    return e;
  }

  private void limit(List<String> errors, Optional<Long> value, long limit, String message) {
    value.ifPresent(v -> limit(errors, v, limit, message));
  }

  private void limit(List<String> errors, long value, long limit, String message) {
    if (value > limit) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }
}
