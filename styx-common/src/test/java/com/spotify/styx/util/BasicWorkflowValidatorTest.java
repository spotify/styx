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

import static com.spotify.styx.testdata.TestData.DOCKER_AND_FLYTE_CONFLICTING_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.FlyteExecConfBuilder;
import com.spotify.styx.model.FlyteIdentifierBuilder;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.testdata.TestData;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class BasicWorkflowValidatorTest {

  private static final int MAX_ID_LENGTH = 256;
  private static final int MAX_RESOURCES = 5;
  private static final int MAX_RESOURCE_LENGTH = 256;
  private static final int MAX_COMMIT_SHA_LENGTH = 256;
  private static final int MAX_SERVICE_ACCOUNT_LENGTH = 256;
  private static final int MAX_RETRY_CONDITION_LENGTH = 256;
  private static final int MAX_ENV_VARS = 128;
  private static final int MAX_ENV_SIZE = 16 * 1024;
  private static final Duration MIN_RUNNING_TIMEOUT = Duration.ofMinutes(1);
  private static final String NOT_VALID_IMAGE = "not-valid-image";

  private static final WorkflowConfiguration INVALID_DOCKER_WORKFLOW_CONFIGURATION =
      WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
          .dockerImage(NOT_VALID_IMAGE)
          .build();

  @Mock
  private DockerImageValidator dockerImageValidator;

  private WorkflowValidator sut;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(dockerImageValidator.validateImageReference(anyString())).thenReturn(Set.of());
    when(dockerImageValidator.validateImageReference(eq(NOT_VALID_IMAGE)))
        .thenReturn(Set.of("error"));
    sut = new BasicWorkflowValidator(dockerImageValidator);
  }

  @Test
  public void validateValidWorkflow() {
    assertThat(sut.validateWorkflow(Workflow.create("test", FULL_WORKFLOW_CONFIGURATION)), is(empty()));
  }

  @Test
  @Parameters({
      "*/15 10 * * 6",
      "* *  *  * *",
      "@hourly", "hourly", "hours",
      "@daily", "daily", "days",
      "@weekly", "weekly", "weeks",
      "@monthly", "monthly", "months",
      "@annually", "annually", "@yearly",
      "yearly", "years",
  })
  public void validateValidCron(String expression) {
    assertThat(sut.validateWorkflow(Workflow.create(
        "test", WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
            .schedule(Schedule.parse(expression))
            .build())),
        is(empty()));
  }

  @Test
  public void validateInvalidOffset() {
    final List<String> errors = sut.validateWorkflow(Workflow.create(
        "test", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET));
    assertThat(errors, hasSize(1));
    assertThat(errors.get(0), startsWith("invalid offset"));
  }
  @Test

  public void validateInvalidDockerImage() {
    var errors = sut.validateWorkflow(Workflow.create("test", INVALID_DOCKER_WORKFLOW_CONFIGURATION));

    assertThat(errors, containsInAnyOrder("invalid image: error"));
  }

  @Test
  public void validateInvalidWorkflow() {
    final String id = Strings.repeat("id", 1024);
    final String schedule = Strings.repeat("schedule", 1024);
    final String offset = Strings.repeat("offset", 1024);
    final String commitSha = Strings.repeat("sha", 1024);
    final List<String> args = IntStream.range(0, 100).mapToObj(i -> "arg-" + i).collect(toList());
    final String serviceAccount = Strings.repeat("account@abc.com", 512);
    final List<String> resources = IntStream.range(0, 10)
        .mapToObj(i -> Strings.repeat("res-" + i, 100)).collect(toList());
    final Map<String, String> env = IntStream.range(0, 2000).boxed()
        .collect(toMap(i -> "env-var-" + i, i -> "env-val-" + i));
    final long envSize = env.entrySet().stream().mapToLong(e -> e.getKey().length() + e.getValue().length()).sum();
    final Duration runningTimeout = Duration.ofSeconds(59L);
    var retryCondition = Strings.repeat("foo -> bar", 512);

    final WorkflowConfiguration invalidConfiguration = WorkflowConfiguration.builder()
        .id(id)
        .schedule(Schedule.parse(schedule))
        .offset(offset)
        .commitSha(commitSha)
        .dockerArgs(args)
        .serviceAccount(serviceAccount)
        .resources(resources)
        .serviceAccount(serviceAccount)
        .env(env)
        .runningTimeout(runningTimeout)
        .retryCondition(retryCondition)
        .build();

    final List<String> errors = sut.validateWorkflow(Workflow.create("test", invalidConfiguration));

    final List<String> expectedErrors = ImmutableList.<String>builder()
        .add(limit("id too long", id.length(), MAX_ID_LENGTH))
        .add("invalid schedule")
        .add(limit("commitSha too long", commitSha.length(), MAX_COMMIT_SHA_LENGTH))
        .add(limit("service account too long", serviceAccount.length(), MAX_SERVICE_ACCOUNT_LENGTH))
        .add(limit("too many resources", resources.size(), MAX_RESOURCES))
        .add(resources.stream().map(r ->
            limit("resource name too long", r.length(), MAX_RESOURCE_LENGTH)).toArray(String[]::new))
        .add("invalid offset: Unable to parse offset period")
        .add(limit("too many env vars", env.size(), MAX_ENV_VARS))
        .add(limit("env too big", envSize, MAX_ENV_SIZE))
        .add(limit("running timeout is too small", runningTimeout, MIN_RUNNING_TIMEOUT))
        .add("service account is not a valid email address: " + serviceAccount)
        .add(limit("retry condition too long", retryCondition.length(), MAX_RETRY_CONDITION_LENGTH))
        .add("invalid retry condition: Expression [" + retryCondition + "] @4: EL1042E: Problem parsing right operand")
        .build();

    assertThat(errors, containsInAnyOrder(expectedErrors.toArray()));
  }

  @Test
  public void shouldNotAllowWorkflowIdMismatch() {
    var component = "test";
    var workflowConfiguration = WorkflowConfiguration.builder()
        .id("foo")
        .schedule(Schedule.HOURS)
        .build();
    @SuppressWarnings("ExtendsAutoValue")
    var workflow = new Workflow() {
      @Override
      public String componentId() {
        return component;
      }

      @Override
      public String workflowId() {
        return "bar";
      }

      @Override
      public WorkflowConfiguration configuration() {
        return workflowConfiguration;
      }
    };
    var errors = sut.validateWorkflow(workflow);
    assertThat(errors, contains("workflow id mismatch"));
  }

  @Test
  public void shouldNotAllowEmptyWorkflowId() {
    var workflowConfiguration = WorkflowConfiguration.builder()
        .id("")
        .schedule(Schedule.HOURS)
        .build();
    var errors = sut.validateWorkflow(Workflow.create("test", workflowConfiguration));
    assertThat(errors, contains("workflow id cannot be empty"));
  }

  @Test
  public void shouldNotAllowEmptyComponent() {
    var errors = sut.validateWorkflow(Workflow.create("", FULL_WORKFLOW_CONFIGURATION));
    assertThat(errors, contains("component id cannot be empty"));
  }

  @Parameters({"foo#bar", "#", "##"})
  @Test
  public void shouldNotAllowComponentWithHash(String component) {
    assertThat(sut.validateWorkflow(Workflow.create(component, FULL_WORKFLOW_CONFIGURATION)),
        contains("component id cannot contain #"));
  }

  @Parameters({"sa@.abc.com", "sa#@abc.com"})
  @Test
  public void shouldRejectInvalidServiceAccount(String serviceAccount) {
    WorkflowConfiguration configuration = WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
            .serviceAccount(serviceAccount)
            .build();

    assertThat(sut.validateWorkflow(Workflow.create("test", configuration)),
            contains("service account is not a valid email address: " + serviceAccount));
  }

  @Test
  public void shouldRejectSpaceServiceAccountWithTailingSpace() {
    String[] invalidServiceAccounts = {"sa@abc.com ", "sa@abc.com\n"};
    // We are looping through the invalid service account list because JUnitParameter removes
    // the trailing spaces and new line but that are the cases we need to test
    for (String serviceAccount : invalidServiceAccounts) {
      WorkflowConfiguration configuration =
          WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
              .serviceAccount(serviceAccount)
              .build();

      assertThat(
          sut.validateWorkflow(Workflow.create("test", configuration)),
          contains("service account is not a valid email address: " + serviceAccount));
      }
  }

  @Parameters({"abc@abc.com", "sa@ab-cd.abc.com", "sa_abc@abc.com"})
  @Test
  public void shouldAcceptValidServiceAccount(String serviceAccount) {
    WorkflowConfiguration configuration = WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
            .serviceAccount(serviceAccount)
            .build();

    assertThat(sut.validateWorkflow(Workflow.create("test", configuration)),
            empty());
  }

  @Test
  public void shouldAcceptValidFlyteExecConf() {
    assertThat(
        sut.validateWorkflow(Workflow.create("test", FLYTE_WORKFLOW_CONFIGURATION)),
        empty()
    );
  }

  @Test
  @TestCaseName("{method} - {1}")
  @Parameters(source = InvalidFlyteConfExecArgsProvider.class)
  public void shouldRejectInvalidFlyteExecConf(WorkflowConfiguration configuration,
                                               String expectedError) {
    assertThat(
        sut.validateWorkflow(Workflow.create("test", configuration)),
        Matchers.allOf(
            Matchers.<String>hasSize(1),
            Matchers.contains(expectedError)
        )
    );
  }

  @Test
  public void shouldRejectConflictingExecConf() {
    assertThat(
        sut.validateWorkflow(Workflow.create("test", DOCKER_AND_FLYTE_CONFLICTING_CONFIGURATION)),
        containsInAnyOrder("configuration cannot specify both docker and flyte parameters")
    );
  }

  public static class InvalidFlyteConfExecArgsProvider {

    private InvalidFlyteConfExecArgsProvider() {}

    @SuppressWarnings("unused")
    public static Object[] provideNotALaunchPlanResource() {
        return new Object[] {
            flyteConf(builder -> builder.resourceType("wf")),
            "only launch plans (\"LAUNCH_PLAN\") are supported as resource type, but received: wf"
        };
    }

    @SuppressWarnings("unused")
    public static Object[] provideEmptyFields() {
      return new Object[] {
          new Object[] { flyteConf(builder -> builder.project("")), "project cannot be empty" },
          new Object[] { flyteConf(builder -> builder.domain("")), "domain cannot be empty" },
          new Object[] { flyteConf(builder -> builder.name("")), "name cannot be empty" },
          new Object[] { flyteConf(builder -> builder.version("")), "version cannot be empty" }
      };
    }

    private static WorkflowConfiguration flyteConf(Consumer<FlyteIdentifierBuilder> identifierMutator) {
      var confBuilder = WorkflowConfigurationBuilder.from(FLYTE_WORKFLOW_CONFIGURATION);
      var flyteExecConf = confBuilder.flyteExecConf().orElseThrow();
      var referenceIdBuilder = FlyteIdentifierBuilder.from(flyteExecConf.referenceId());
      identifierMutator.accept(referenceIdBuilder);
      return confBuilder.flyteExecConf(
          FlyteExecConfBuilder.from(flyteExecConf)
              .referenceId(referenceIdBuilder.build())
              .build())
          .build();
    }
  }

  private String limit(String msg, Object value, Object limit) {
    return msg + ": " + value + ", limit = " + limit;
  }
}
