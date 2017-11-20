/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.cli;

import static com.spotify.futures.CompletableFutures.exceptionallyCompletedFuture;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.cli.CliExitException.ExitStatus;
import com.spotify.styx.cli.CliMain.CliContext;
import com.spotify.styx.client.ApiErrorException;
import com.spotify.styx.client.ClientErrorException;
import com.spotify.styx.client.StyxClient;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.serialization.Json;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CliMainTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock CliContext cliContext;
  @Mock StyxClient client;
  @Mock CliOutput cliOutput;

  @Before
  public void setUp() throws Exception {
    when(cliContext.createClient(any())).thenReturn(client);
    when(cliContext.output(any())).thenReturn(cliOutput);
    when(cliContext.env()).thenReturn(
        ImmutableMap.of("STYX_CLI_HOST", "https://styx.foo.bar:4711"));
  }

  @Test
  public void testList() throws Exception {
    final RunStateDataPayload payload = mock(RunStateDataPayload.class);
    when(client.activeStates(Optional.empty()))
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "ls");
    verify(client).activeStates(Optional.empty());
    verify(cliOutput).printStates(payload);
  }

  public void testWorkflowCreate() throws Exception {
    final String component = "quux";

    final Path workflowsFile = fileFromResource("workflows.yaml");
    final List<WorkflowConfiguration> expected = Json.YAML_MAPPER
        .reader().forType(WorkflowConfiguration.class)
        .<WorkflowConfiguration>readValues(workflowsFile.toFile())
        .readAll();
    assertThat(expected, is(not(Matchers.empty())));

    when(client.createOrUpdateWorkflow(any(), any())).thenAnswer(a -> {
      final String comp = a.getArgumentAt(0, String.class);
      final WorkflowConfiguration wfConfig = a.getArgumentAt(1, WorkflowConfiguration.class);
      return CompletableFuture.completedFuture(Workflow.create(comp, wfConfig));
    });

    CliMain.run(cliContext, "workflow", "create", component, "-f", workflowsFile.toString());

    for (WorkflowConfiguration workflowConfiguration : expected) {
      verify(client).createOrUpdateWorkflow(component, workflowConfiguration);
      verify(cliOutput).printMessage("Workflow " + workflowConfiguration.id() + " in component "
          + component + " created.");
    }
  }

  @Test
  public void testWorkflowDelete() throws Exception {
    final String component = "quux";

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", "quux", "foo", "bar");

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testClientError() throws Exception {
    final ClientErrorException exception = new ClientErrorException(
        "foo failure", new IOException());
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenReturn(exceptionallyCompletedFuture(exception));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ClientError));
    }

    verify(cliOutput).printError("Client error: " + exception.getMessage());
  }

  @Test
  public void testApiError() throws Exception {
    final ApiErrorException exception = new ApiErrorException("bar failure", 500, true);
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenReturn(exceptionallyCompletedFuture(exception));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ApiError));
    }

    verify(cliOutput).printError("API error: " + exception.getMessage());
  }

  @Test
  public void testClientUnknownError() throws Exception {
    final NullPointerException exception = new NullPointerException();
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenReturn(CompletableFutures.exceptionallyCompletedFuture(exception));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ClientError));
    }

    verify(cliOutput).printError(Throwables.getStackTraceAsString(exception));
  }


  @Test
  public void testUnknownError() throws Exception {
    final NullPointerException exception = new NullPointerException();
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenThrow(exception);

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }

    verify(cliOutput).printError(Throwables.getStackTraceAsString(exception));
  }

  @Test
  public void testMissingCredentialsHelpMessage() throws Exception {
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenReturn(exceptionallyCompletedFuture(new ApiErrorException("foo", 401, false)));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.AuthError));
    }

    verify(cliOutput).printError(contains("gcloud auth application-default login"));
  }

  @Test
  public void testUnauthorizedMessage() throws Exception {
    when(client.triggerWorkflowInstance(any(), any(), any()))
        .thenReturn(exceptionallyCompletedFuture(new ApiErrorException("foo", 401, true)));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.AuthError));
    }

    verify(cliOutput).printError("API error: Unauthorized");
  }

  @Test
  public void testHelp() throws Exception {
    try {
      CliMain.run(cliContext, "--help");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.Success));
    }

    try {
      CliMain.run(cliContext);
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.Success));
    }

    verifyZeroInteractions(client);
  }

  @Test
  public void testArgumentError() throws Exception {
    try {
      CliMain.run(cliContext, "foozbarz");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ArgumentError));
    }

    verifyZeroInteractions(client);
  }

  private Path fileFromResource(String name) throws IOException {
    final File workflowsFile = temporaryFolder.newFile();
    try (OutputStream os = Files.newOutputStream(workflowsFile.toPath())) {
      Resources.copy(Resources.getResource(this.getClass(), name), os);
    }
    return workflowsFile.toPath();
  }
}
