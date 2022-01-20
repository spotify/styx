/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.model;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.testdata.TestData.DAILY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.HOURLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET;
import static com.spotify.styx.testdata.TestData.MONTHLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WEEKLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.YEARLY_WORKFLOW_CONFIGURATION;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class WorkflowConfigurationTest {

  private static final Instant NOW = Instant.now();

  @Test
  public void shouldAddOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET.addOffset(NOW),
        is(NOW.plus(30, DAYS).plus(30, MINUTES)));
  }

  @Test
  public void shouldAddDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.addOffset(NOW),
        is(NOW.plus(1, HOURS)));
  }

  @Test
  public void shouldSubtractOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET.subtractOffset(NOW),
        is(NOW.minus(30, DAYS).minus(30, MINUTES)));
  }

  @Test
  public void shouldSubtractDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.subtractOffset(NOW),
        is(NOW.minus(1, HOURS)));
  }

  @Test
  public void shouldReturnDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("PT1H"));
    assertThat(DAILY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1D"));
    assertThat(WEEKLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1W"));
    assertThat(MONTHLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1M"));
    assertThat(YEARLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1Y"));
    assertThat(WorkflowConfigurationBuilder.from(HOURLY_WORKFLOW_CONFIGURATION)
            .schedule(Schedule.parse("45 23 * * 6"))
            .build()
            .defaultOffset(),
        is("PT0S"));
  }

  @Test
  public void shouldWorkWithValidDeploymentSource() {

  }

  @Test
  @TestCaseName("{method}: {0}")
  @Parameters(source = ParseJsonArgsProvider.class)
  public void shouldParseFromJson(String testCase, String json, WorkflowConfiguration expected)
      throws JsonProcessingException {
    final var conf = OBJECT_MAPPER.readValue(json, WorkflowConfiguration.class);

    assertThat(conf, is(expected));
  }

  public static class ParseJsonArgsProvider {

    private ParseJsonArgsProvider(){
      // Prevents instantiation
    }

    @SuppressWarnings("unused")
    public static Object[] provideDockerConf() {
      return new Object[] {
          "Original docker centred conf",
          buildJson(
              "\"docker_image\":\"gcr.io/some-bucket/some-image\","
              + "\"docker_args\":[\"1\",\"2\",\"3\"],"
              + "\"docker_termination_logging\":true"),
          configurationBuilder()
              .dockerImage("gcr.io/some-bucket/some-image")
              .dockerArgs(Arrays.asList("1", "2", "3"))
              .dockerTerminationLogging(true)
              .build()
      };
    }

    @SuppressWarnings("unused")
    public static Object[] provideFlyteExecConf() {
      return new Object[] {
          "Flyte exec conf",
          buildJson(
              "\"flyte_exec_conf\":{"
              + "  \"reference_id\":{"
              + "    \"resource_type\":\"launch-plan\","
              + "    \"domain\":\"production\","
              + "    \"project\":\"flyte-test\","
              + "    \"name\":\"TestWorkflow\","
              + "    \"version\":\"1.0\""
              + "  },"
              + "  \"input_fields\":{\"foo\": \"bar\"}"
              + "}"),
          configurationBuilder()
              .flyteExecConf(FlyteExecConf.builder()
                  .referenceId(FlyteIdentifier.builder()
                      .resourceType("launch-plan")
                      .domain("production")
                      .project("flyte-test")
                      .name("TestWorkflow")
                      .version("1.0")
                      .build())
                  .inputFields("foo", "bar")
                  .build())
              .build()
      };
    }

    @SuppressWarnings("unused")
    public static Object[] provideDeploymentSource() {
      return new Object[] {
          "Original docker centred conf",
          buildJson(
              "\"docker_image\":\"gcr.io/some-bucket/some-image\","
              + "\"docker_args\":[\"1\",\"2\",\"3\"],"
              + "\"docker_termination_logging\":true,"
              + "\"source\":{\"repository\":\"some-organisation/some-path-to-repositry\", \"source\":\"some-tool-name/some/path/to/file\" }"
          ),
          configurationBuilder()
              .dockerImage("gcr.io/some-bucket/some-image")
              .dockerArgs(Arrays.asList("1", "2", "3"))
              .dockerTerminationLogging(true)
              .source(
                  DeploymentSource.builder()
                      .repository("some-organisation/some-path-to-repositry")
                      .source("some-tool-name/some/path/to/file")
                      .build())
              .build()
      };
    }

    @SuppressWarnings("unused")
    public static Object[] providePartialDeploymentSource() {
      return new Object[] {
          "Original docker centred conf",
          buildJson(
              "\"docker_image\":\"gcr.io/some-bucket/some-image\","
              + "\"docker_args\":[\"1\",\"2\",\"3\"],"
              + "\"docker_termination_logging\":true,"
              + "\"source\":{\"source\":\"some-tool-name/some/path/to/file\" }"
          ),
          configurationBuilder()
              .dockerImage("gcr.io/some-bucket/some-image")
              .dockerArgs(Arrays.asList("1", "2", "3"))
              .dockerTerminationLogging(true)
              .source(
                  DeploymentSource.builder()
                      .source("some-tool-name/some/path/to/file")
                      .build())
              .build()
      };
    }

    private static WorkflowConfigurationBuilder configurationBuilder() {
      return WorkflowConfiguration.builder()
          .id("id")
          .schedule(Schedule.DAYS)
          .offset("P1D")
          .commitSha("817cdc3f95382c4e4e11232e3a2de484de8a2bea")
          .serviceAccount("foo@project.com")
          .resources(Collections.singletonList("resource"))
          .env(Collections.singletonMap("STYX_FOO", "bar"))
          .runningTimeout(Duration.ofHours(3))
          .retryCondition("true");
    }

    public static String buildJson(String execConf) {
      return "{"
             + "\"id\":\"id\","
             + "\"schedule\":\"days\","
             + "\"offset\":\"P1D\","
             + "\"commit_sha\":\"817cdc3f95382c4e4e11232e3a2de484de8a2bea\","
             + "\"service_account\":\"foo@project.com\","
             + "\"resources\":[\"resource\"],"
             + "\"env\":{\"STYX_FOO\":\"bar\"},"
             + "\"running_timeout\":10800.000000000,"
             + "\"retry_condition\":\"true\","
             + execConf
             + "}";
    }
  }
}
