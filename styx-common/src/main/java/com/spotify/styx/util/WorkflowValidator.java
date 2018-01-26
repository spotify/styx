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

  static final int MAX_ID_LENGTH = 256;
  static final int MAX_DOCKER_ARGS_TOTAL = 1024;
  static final int MAX_RESOURCES = 5;
  static final int MAX_RESOURCE_LENGTH = 256;
  static final int MAX_COMMIT_SHA_LENGTH = 256;
  static final int MAX_SECRET_NAME_LENGTH = 253;
  static final int MAX_SECRET_MOUNT_PATH_LENGTH = 4096;
  static final int MAX_SERVICE_ACCOUNT_LENGTH = 256;

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

    limit(e, cfg.id().length(), MAX_ID_LENGTH, "id too long");
    limit(e, cfg.commitSha().map(String::length), MAX_COMMIT_SHA_LENGTH, "commitSha too long");
    limit(e, cfg.secret().map(s -> s.name().length()), MAX_SECRET_NAME_LENGTH, "secret name too long");
    limit(e, cfg.secret().map(s -> s.mountPath().length()), MAX_SECRET_MOUNT_PATH_LENGTH, "secret mount path too long");
    limit(e, cfg.serviceAccount().map(String::length), MAX_SERVICE_ACCOUNT_LENGTH, "service account too long");
    limit(e, cfg.resources().size(), MAX_RESOURCES, "too many resources");

    cfg.dockerImage().ifPresent(image ->
        dockerImageValidator.validateImageReference(image).stream()
            .map(s -> "invalid image: " + s)
            .forEach(e::add));

    cfg.resources().stream().map(String::length).forEach(v ->
        limit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    limit(e, cfg.dockerArgs().map(args -> args.size() + args.stream().mapToInt(String::length).sum()),
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

  private void limit(List<String> errors, Optional<Integer> value, int limit, String message) {
    value.ifPresent(v -> limit(errors, v, limit, message));
  }

  private void limit(List<String> errors, int value, int limit, String message) {
    if (value > limit) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }
}
