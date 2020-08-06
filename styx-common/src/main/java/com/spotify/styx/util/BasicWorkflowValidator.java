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

import static com.spotify.styx.util.WorkflowValidator.lowerLimit;
import static com.spotify.styx.util.WorkflowValidator.upperLimit;
import static java.lang.String.format;

import com.spotify.styx.model.DockerExecConf;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class BasicWorkflowValidator implements WorkflowValidator {

  private static final int MAX_ID_LENGTH = 256;
  private static final int MAX_DOCKER_ARGS_TOTAL = 1000000;
  private static final int MAX_RESOURCES = 5;
  private static final int MAX_RESOURCE_LENGTH = 256;
  private static final int MAX_COMMIT_SHA_LENGTH = 256;
  private static final int MAX_SECRET_NAME_LENGTH = 253;
  private static final int MAX_SECRET_MOUNT_PATH_LENGTH = 1024;
  private static final int MAX_SERVICE_ACCOUNT_LENGTH = 256;
  private static final int MAX_RETRY_CONDITION_LENGTH = 256;
  private static final int MAX_ENV_VARS = 128;
  private static final int MAX_ENV_SIZE = 16 * 1024;
  private static final Pattern VALID_EMAIL_ADDRESS_REGEX =
      Pattern.compile("^[_A-Za-z0-9-+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$",
          Pattern.CASE_INSENSITIVE);

  private static final Duration MIN_RUNNING_TIMEOUT = Duration.ofMinutes(1);
  private final DockerImageValidator dockerImageValidator;

  public BasicWorkflowValidator(DockerImageValidator dockerImageValidator) {
    this.dockerImageValidator = Objects.requireNonNull(dockerImageValidator);
  }

  @Override
  public List<String> validateWorkflow(Workflow workflow) {
    var workflowId = workflow.id();
    var cfg = workflow.configuration();

    final List<String> e = new ArrayList<>();

    var componentId = workflowId.componentId();

    empty(e, "component id", workflowId.componentId());
    if (componentId.contains("#")) {
      e.add("component id cannot contain #");
    }

    empty(e, "workflow id", workflowId.id());

    if (!workflowId.id().equals(cfg.id())) {
      e.add("workflow id mismatch");
    }

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
    upperLimit(e, cfg.retryCondition().map(String::length).orElse(0),
        MAX_RETRY_CONDITION_LENGTH, "retry condition too long");

    cfg.resources().stream().map(String::length).forEach(v ->
        upperLimit(e, v, MAX_RESOURCE_LENGTH, "resource name too long"));

    cfg.offset().ifPresent(offset -> {
      try {
        TimeUtil.addOffset(ZonedDateTime.now(ZoneOffset.UTC), offset);
      } catch (DateTimeParseException ex) {
        e.add(format("invalid offset: %s", ex.getMessage()));
      }
    });

    try {
      TimeUtil.cron(cfg.schedule());
    } catch (IllegalArgumentException ex) {
      e.add("invalid schedule");
    }

    cfg.runningTimeout().ifPresent(timeout ->
        lowerLimit(e, timeout, MIN_RUNNING_TIMEOUT, "running timeout is too small"));

    cfg.serviceAccount().ifPresent(serviceAccount -> {
      if (!validateServiceAccount(serviceAccount)) {
        e.add("service account is not a valid email address: " + serviceAccount);
      }
    });

    cfg.retryCondition().ifPresent(retryCondition -> {
      try {
        new SpelExpressionParser().parseRaw(retryCondition);
      } catch (RuntimeException ex) {
        e.add(format("invalid retry condition: %s", ex.getMessage()));
      }
    });

    validateDockerExecConf(e, cfg);
    validateFlyteExecConf(e, cfg);
    validateConflictingExecConf(e, cfg);

    return e;
  }

  private static boolean validateServiceAccount(String serviceAccount) {
    var matcher = VALID_EMAIL_ADDRESS_REGEX.matcher(serviceAccount);
    return matcher.matches();
  }

  private void validateDockerExecConf(List<String> errors, WorkflowConfiguration config) {
    validateDockerParam(errors, "dockerImage", config.dockerImage(),
        config.dockerExecConf().flatMap(DockerExecConf::dockerImage),
        this::validateDockerImage);
    validateDockerParam(errors, "dockerArgs", config.dockerArgs(),
        config.dockerExecConf().flatMap(DockerExecConf::dockerArgs),
        this::validateDockerArgs);

    // not verifying dockerTerminationLogging as it defaults to false
    // So for
    //       WorkflowConfiguration.builder()
    //          .id("styx.TestEndpoint")
    //          .commitSha(VALID_SHA)
    //          .schedule(DAYS)
    //          .serviceAccount("foo@bar.baz.quux")
    //          .dockerExecConf(new DockerExecConfBuilder()
    //              .dockerImage("busybox")
    //              .dockerArgs(List.of("x", "y"))
    //              .dockerTerminationLogging(true)
    //              .build())
    //          .build();
    // will fail as default at upper level is false but inside dockerExecConf is set to true
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private <T> void validateDockerParam(List<String> errors, String paramName, Optional<T> valueA, Optional<T> valueB,
                                       BiConsumer<List<String>, T> validateFunction) {
    valueA.ifPresent(a -> validateFunction.accept(errors, a));
    valueB.ifPresent(b -> validateFunction.accept(errors, b));

    valueA.ifPresent(a ->
        valueB.ifPresent(b -> same(errors, paramName, a, b))
    );
  }

  private void validateDockerImage(List<String> errors, String image) {
    dockerImageValidator.validateImageReference(image).stream()
        .map(s -> "invalid image: " + s)
        .forEach(errors::add);
  }

  private void validateDockerArgs(List<String> errors, List<String> args) {
    final int dockerArgs = args.size() + args.stream().mapToInt(String::length).sum();
    upperLimit(errors, dockerArgs, MAX_DOCKER_ARGS_TOTAL, "docker args is too large");
  }

  private void validateFlyteExecConf(List<String> errors, WorkflowConfiguration config) {
    config.flyteExecConf().ifPresent(flyteExecConf -> {
      var flyteIdentifier = flyteExecConf.referenceId();

      var resourceType = flyteIdentifier.resourceType();
      if (!"lp".equals(resourceType)) {
        errors.add("only launch plans (\"lp\") are supported: " + resourceType);
      }

      empty(errors, "domain", flyteIdentifier.domain());
      empty(errors, "project", flyteIdentifier.project());
      empty(errors, "name", flyteIdentifier.name());
      empty(errors, "version", flyteIdentifier.version());
    });
  }

  private void validateConflictingExecConf(List<String> e, WorkflowConfiguration cfg) {
    var hasDockerConf = cfg.dockerExecConf().isPresent() || cfg.dockerImage().isPresent()
                        || cfg.dockerArgs().isPresent();
    if (cfg.flyteExecConf().isPresent() && hasDockerConf) {
      e.add("configuration cannot specify both docker and flyte parameters");
    }
  }

  private void empty(List<String> errors, String fieldName, String value) {
    if (value.isEmpty()) {
      errors.add(fieldName + " cannot be empty");
    }
  }

  private <T> void same(List<String> errors, String fieldName, T valueA, T valueB) {
    if (!Objects.equals(valueA, valueB)) {
      errors.add(String.format("mismatching %s configuration: \"%s\" != \"%s\"", fieldName, valueA, valueB));
    }
  }
}
