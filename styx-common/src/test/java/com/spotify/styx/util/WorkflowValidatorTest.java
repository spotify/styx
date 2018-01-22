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

import static com.spotify.styx.util.WorkflowValidator.MAX_COMMIT_SHA_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_ID_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_RESOURCES;
import static com.spotify.styx.util.WorkflowValidator.MAX_RESOURCE_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_SCHEDULE_EXPRESSION_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_SECRET_MOUNT_PATH_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_SECRET_NAME_LENGTH;
import static com.spotify.styx.util.WorkflowValidator.MAX_SERVICE_ACCOUNT_LENGTH;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.testdata.TestData;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkflowValidatorTest {

  @Mock DockerImageValidator dockerImageValidator;

  private WorkflowValidator sut;

  @Before
  public void setUp() throws Exception {
    when(dockerImageValidator.validateImageReference(anyString())).thenReturn(Collections.emptyList());
    sut = new WorkflowValidator(dockerImageValidator);
  }

  @Test
  public void validateValidWorkflow() throws Exception {
    assertThat(sut.validateWorkflowConfiguration(TestData.FULL_WORKFLOW_CONFIGURATION), is(empty()));
  }

  @Test
  public void validateInvalidOffset() throws Exception {
    final List<String> errors = sut.validateWorkflowConfiguration(
        TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET);
    assertThat(errors, hasSize(1));
    assertThat(errors.get(0), startsWith("invalid offset"));
  }

  @Test
  public void validateInvalidDockerImage() throws Exception {
    when(dockerImageValidator.validateImageReference(anyString())).thenReturn(ImmutableList.of("foo", "bar"));
    final List<String> errors = sut.validateWorkflowConfiguration(TestData.FULL_WORKFLOW_CONFIGURATION);
    assertThat(errors, contains("invalid image: foo", "invalid image: bar"));
  }


  @Test
  public void validateInvalidWorkflow() throws Exception {
    final String id = Strings.repeat("id", 1024);
    final String schedule = Strings.repeat("schedule", 1024);
    final String offset = Strings.repeat("offset", 1024);
    final String commitSha = Strings.repeat("sha", 1024);
    final List<String> args = IntStream.range(0, 100).mapToObj(i -> "arg-" + i).collect(toList());
    final Secret secret = Secret.create(Strings.repeat("foo", 1024), Strings.repeat("bar", 4711));
    final String serviceAccount = Strings.repeat("account", 1024);
    final List<String> resources = IntStream.range(0, 10)
        .mapToObj(i -> Strings.repeat("res-" + i, 100)).collect(toList());

    final WorkflowConfiguration invalidConfiguration = WorkflowConfiguration.builder()
        .id(id)
        .schedule(Schedule.parse(schedule))
        .offset(offset)
        .commitSha(commitSha)
        .dockerArgs(args)
        .secret(secret)
        .serviceAccount(serviceAccount)
        .resources(resources)
        .serviceAccount(serviceAccount)
        .build();

    final List<String> errors = sut.validateWorkflowConfiguration(invalidConfiguration);

    final List<String> expectedErrors = ImmutableList.<String>builder()
        .add(limit("id too long", id.length(), MAX_ID_LENGTH))
        .add(limit("schedule expression too long", schedule.length(), MAX_SCHEDULE_EXPRESSION_LENGTH))
        .add(limit("commitSha too long", commitSha.length(), MAX_COMMIT_SHA_LENGTH))
        .add(limit("secret name too long", secret.name().length(), MAX_SECRET_NAME_LENGTH))
        .add(limit("secret mount path too long", secret.mountPath().length(), MAX_SECRET_MOUNT_PATH_LENGTH))
        .add(limit("service account too long", serviceAccount.length(), MAX_SERVICE_ACCOUNT_LENGTH))
        .add(limit("too many resources", resources.size(), MAX_RESOURCES))
        .add(resources.stream().map(r ->
            limit("resource name too long", r.length(), MAX_RESOURCE_LENGTH)).toArray(String[]::new))
        .add("invalid offset: Text cannot be parsed to a Period")
        .build();

    assertThat(errors, containsInAnyOrder(expectedErrors.toArray()));
  }

  private String limit(String msg, int value, int limit) {
    return msg + ": " + value + ", limit = " + limit;
  }
}
