/*-
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

import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExtendedWorkflowValidatorTest {

  private static final Duration MAX_RUNNING_TIMEOUT = Duration.ofDays(1);

  @Mock private WorkflowValidator basicWorkflowValidator;

  private WorkflowValidator sut;

  @Before
  public void setUp() throws Exception {
    when(basicWorkflowValidator.validateWorkflow(any())).thenReturn(List.of());
    sut = new ExtendedWorkflowValidator(basicWorkflowValidator, Duration.ofDays(1),
        Set.of(FULL_WORKFLOW_CONFIGURATION.secret().orElseThrow().name()));
  }

  @Test
  public void validateValidWorkflow() {
    assertThat(sut.validateWorkflow(Workflow.create("test", FULL_WORKFLOW_CONFIGURATION)), is(empty()));
  }

  @Test
  public void shouldPropagateErrorFromDelegate() {
    var errors = List.of("foo", "bar");
    when(basicWorkflowValidator.validateWorkflow(any())).thenReturn(errors);

    assertThat(sut.validateWorkflow(Workflow.create("test", FULL_WORKFLOW_CONFIGURATION)), is(errors));
  }

  @Test
  public void shouldEnforceMaxRunningTimeoutLimitWhenSpecified() {
    var runningTimeout = Duration.ofDays(365);

    var errors = sut.validateWorkflow(
        Workflow.create("test", WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
            .runningTimeout(runningTimeout)
            .build()));

    assertThat(errors,
        contains(limit("running timeout is too big", runningTimeout, MAX_RUNNING_TIMEOUT)));
  }

  @Test
  public void shouldFailUsageOfNonWhitelistedSecret() {
    var errors = sut.validateWorkflow(
        Workflow.create("test", WorkflowConfigurationBuilder.from(FULL_WORKFLOW_CONFIGURATION)
            .secret(WorkflowConfiguration.Secret.create("foo-secret", "/path"))
            .build()));

    assertThat(errors, contains("secret foo-secret is not whitelisted"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfInvalidMaxRunningTimeout() {
    new ExtendedWorkflowValidator(basicWorkflowValidator, Duration.ofDays(-1), Set.of());
  }

  private String limit(String msg, Object value, Object limit) {
    return msg + ": " + value + ", limit = " + limit;
  }
}
