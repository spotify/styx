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
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class WorkflowValidator {

  static final int MAX_ID_LENGTH = 256;
  static final int MAX_DOCKER_ARGS_TOTAL = 1000000;
  static final int MAX_RESOURCES = 5;
  static final int MAX_RESOURCE_LENGTH = 256;
  static final int MAX_COMMIT_SHA_LENGTH = 256;
  static final int MAX_SECRET_NAME_LENGTH = 253;
  static final int MAX_SECRET_MOUNT_PATH_LENGTH = 1024;
  static final int MAX_SERVICE_ACCOUNT_LENGTH = 256;
  static final int MAX_ENV_VARS = 128;
  static final int MAX_ENV_SIZE = 16 * 1024;
  static final Duration MIN_RUNNING_TIMEOUT = Duration.ofMinutes(1);

  private final DockerImageValidator dockerImageValidator;
  private final Duration maybeMaxRunningTimeout;
  private final Set<String> secretWhitelist;

  private static final Pattern VALID_EMAIL_ADDRESS_REGEX =
      Pattern.compile(
              "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$", Pattern.CASE_INSENSITIVE);

  private WorkflowValidator(DockerImageValidator dockerImageValidator, Duration maybeMaxRunningTimeout,
                            Set<String> secretWhitelist) {

    Preconditions.checkArgument(maybeMaxRunningTimeout == null || !maybeMaxRunningTimeout.isNegative(),
        "Max Running timeout should be positive");
    this.dockerImageValidator = Objects.requireNonNull(dockerImageValidator);
    this.maybeMaxRunningTimeout = maybeMaxRunningTimeout;
    this.secretWhitelist = secretWhitelist;
  }

  public static Builder newBuilder(DockerImageValidator dockerImageValidator) {
    return new Builder(dockerImageValidator);
  }

  public List<String> validateWorkflow(Workflow workflow) {
    var workflowId = workflow.id();
    var cfg = workflow.configuration();

    final List<String> e = new ArrayList<>();

    var componentId = workflowId.componentId();
    if (componentId.isEmpty()) {
      e.add("component id cannot be empty");
    } else if (componentId.contains("#")) {
      e.add("component id cannot contain #");
    }

    if (workflowId.id().isEmpty()) {
      e.add("workflow id cannot be empty");
    }

    if (!workflowId.id().equals(cfg.id())) {
      e.add("workflow id mismatch");
    }

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
            .mapToInt(entry -> entry.getKey().length() + entry.getValue().length()).sum(),
        MAX_ENV_SIZE, "env too big");

    cfg.dockerImage().ifPresent(image ->
        dockerImageValidator.validateImageReference(image).stream()
            .map(s -> "invalid image: " + s)
            .forEach(e::add));

    cfg.resources().stream().map(String::length).forEach(v ->
        upperLimit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    cfg.dockerArgs().ifPresent(args -> {
      final int dockerArgs = args.size() + args.stream().mapToInt(String::length).sum();
      upperLimit(e, dockerArgs, MAX_DOCKER_ARGS_TOTAL, "docker args is too large");
    });

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

    cfg.runningTimeout().ifPresent(timeout -> {
      lowerLimit(e, timeout, MIN_RUNNING_TIMEOUT, "running timeout is too small");
      if (maybeMaxRunningTimeout != null) {
        upperLimit(e, timeout, maybeMaxRunningTimeout, "running timeout is too big");
      }
    });

    cfg.secret().ifPresent(secret -> {
      if (secretWhitelist != null && !secretWhitelist.contains(secret.name())) {
        e.add("secret " + secret.name() + " is not whitelisted");
      }
    });

    cfg.serviceAccount().ifPresent(serviceAccount -> {
      if (!validateServiceAccount(serviceAccount)) {
        e.add("service account is not a valid email address: " + serviceAccount);
      }
    });

    return e;
  }

  private <T extends Comparable<T>> void lowerLimit(List<String> errors, T value, T limit, String message) {
    limit(errors,value.compareTo(limit) < 0, value, limit, message);
  }

  private <T extends Comparable<T>> void upperLimit(List<String> errors, T value, T limit, String message) {
    limit(errors,value.compareTo(limit) > 0, value, limit, message);
  }

  private <T extends Comparable<T>> void limit(List<String> errors, boolean isError, T value, T limit, String message) {
    if (isError) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }

  private static boolean validateServiceAccount(String serviceAccount) {
    var matcher = VALID_EMAIL_ADDRESS_REGEX .matcher(serviceAccount);
    return matcher.matches();
  }

  public static class Builder {
    private DockerImageValidator dockerImageValidator;
    private Duration maxRunningTimeout;
    private Set<String> secretWhitelist;

    public Builder(DockerImageValidator dockerImageValidator) {
      this.dockerImageValidator = dockerImageValidator;
    }

    public Builder withMaxRunningTimeoutLimit(Duration maxRunningTimeout) {
      this.maxRunningTimeout = maxRunningTimeout;
      return this;
    }

    public Builder withSecretWhitelist(Set<String> secretWhitelist) {
      this.secretWhitelist = secretWhitelist;
      return this;
    }

    public WorkflowValidator build() {
      return new WorkflowValidator(dockerImageValidator, maxRunningTimeout, secretWhitelist);
    }
  }
}
