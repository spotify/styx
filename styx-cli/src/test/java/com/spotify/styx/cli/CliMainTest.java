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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.cli.CliExitException.ExitStatus;
import com.spotify.styx.cli.CliMain.CliContext;
import com.spotify.styx.client.ApiErrorException;
import com.spotify.styx.client.ClientErrorException;
import com.spotify.styx.client.StyxClient;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.serialization.Json;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class CliMainTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock CliContext cliContext;
  @Mock StyxClient client;
  @Mock CliOutput cliOutput;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(cliContext.createClient(any(), any())).thenReturn(client);
    when(cliContext.output(any())).thenReturn(cliOutput);
    when(cliContext.env()).thenReturn(
        ImmutableMap.of("STYX_CLI_HOST", "https://styx.foo.bar:4711"));
  }

  @Test
  public void testList() {
    final RunStateDataPayload payload = mock(RunStateDataPayload.class);
    when(client.activeStates(Optional.empty()))
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "ls");
    verify(client).activeStates(Optional.empty());
    verify(cliOutput).printStates(payload);
  }

  private void testWorkflowCreate() throws Exception {
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
  public void testWorkflowDelete() {
    final String component = "quux";

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar");

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testBackfillCreate() throws Exception {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    when(client.backfillCreate(component, "foo", start,
        end, 1, null))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30",
        "1");

    verify(client).backfillCreate(component, "foo", start, end, 1, null);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillCreateWithDescription() throws Exception {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    when(client.backfillCreate(component, "foo", start,
        end, 1, "Description"))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30",
        "1", "-d", "Description");

    verify(client).backfillCreate(component, "foo", start, end, 1, "Description");
    verify(cliOutput).printBackfill(backfill, true);
  }
  
  @Test
  public void testBackfillShow() throws Exception {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();
    
    final BackfillPayload backfillPayload = BackfillPayload.create(backfill,
        Optional.empty());

    when(client.backfill(backfillId, true))
        .thenReturn(CompletableFuture.completedFuture(backfillPayload));

    CliMain.run(cliContext, "backfill", "show", backfillId, "--no-trunc");
    
    verify(client).backfill(backfillId, true);
    verify(cliOutput).printBackfillPayload(backfillPayload, true);
  }

  @Test
  public void testBackfillShowTruncating() throws Exception {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();
    
    final BackfillPayload backfillPayload = BackfillPayload.create(backfill,
        Optional.empty());

    when(client.backfill(backfillId, true))
        .thenReturn(CompletableFuture.completedFuture(backfillPayload));

    CliMain.run(cliContext, "backfill", "show", backfillId);
    
    verify(client).backfill(backfillId, true);
    verify(cliOutput).printBackfillPayload(backfillPayload, false);
  }

  @Test
  public void testBackfillEdit() throws Exception {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    when(client.backfillEditConcurrency(backfillId, 1))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "edit", backfillId, "--concurrency", "1");

    verify(client).backfillEditConcurrency(backfillId, 1);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillList() throws Exception {
    final String component = "quux";
    final String workflow = "foo";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, workflow))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();
    
    final BackfillsPayload backfillsPayload = BackfillsPayload.create(
        ImmutableList.of(BackfillPayload.create(backfill, Optional.empty())));

    when(client.backfillList(Optional.of(component), Optional.of(workflow), false, false))
        .thenReturn(CompletableFuture.completedFuture(backfillsPayload));

    CliMain.run(cliContext, "backfill", "list", "-c", component, "-w", workflow, "--no-trunc");

    verify(client).backfillList(Optional.of(component), Optional.of(workflow), false, false);
    verify(cliOutput).printBackfills(backfillsPayload.backfills(), true);
  }

  @Test
  public void testBackfillListTruncating() throws Exception {
    final String component = "quux";
    final String workflow = "foo";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, workflow))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();
    
    final BackfillsPayload backfillsPayload = BackfillsPayload.create(
        ImmutableList.of(BackfillPayload.create(backfill, Optional.empty())));

    when(client.backfillList(Optional.of(component), Optional.of(workflow), false, false))
        .thenReturn(CompletableFuture.completedFuture(backfillsPayload));

    CliMain.run(cliContext, "backfill", "list", "-c", component, "-w", workflow);

    verify(client).backfillList(Optional.of(component), Optional.of(workflow), false, false);
    verify(cliOutput).printBackfills(backfillsPayload.backfills(), false);
  }

  @Test
  @Parameters({
      "n",
      "N",
      "Y",
      "",
      "  ",
      "dfgdfgd",
  })
  public void testWorkflowDeleteInteractiveNo(String reply) {
    when(cliContext.hasConsole()).thenReturn(true);
    when(cliContext.consoleReadLine(any())).thenReturn(reply);

    try {
      CliMain.run(cliContext, "workflow", "delete", "quux", "foo", "bar");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }

    verify(cliContext).consoleReadLine(
        "Sure you want to delete the workflows foo, bar in component quux? [y/N] ");
  }

  @Test
  @Parameters({
      "y",
      " y",
      "y ",
      " y ",
  })
  public void testWorkflowDeleteInteractiveYes(String reply) {
    final String component = "quux";

    when(cliContext.hasConsole()).thenReturn(true);
    when(cliContext.consoleReadLine(any())).thenReturn(reply);

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar");

    verify(cliContext).consoleReadLine(
        "Sure you want to delete the workflows foo, bar in component quux? [y/N] ");

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testWorkflowDeleteInteractiveForce() {
    final String component = "quux";

    when(cliContext.hasConsole()).thenReturn(true);

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar", "--force");

    verify(cliContext, never()).consoleReadLine(any());

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testWorkflowEnable() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .build();

    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(CompletableFuture.completedFuture(workflowState));

    CliMain.run(cliContext, "workflow", "enable", component, "foo", "bar");

    verify(client).updateWorkflowState(component, "foo", workflowState);
    verify(client).updateWorkflowState(component, "bar", workflowState);
    verify(cliOutput).printMessage("Workflow foo in component " + component + " enabled.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " enabled.");
  }

  @Test
  public void testWorkflowDisable() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(false)
        .build();

    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(CompletableFuture.completedFuture(workflowState));

    CliMain.run(cliContext, "workflow", "disable", component, "foo", "bar");

    verify(client).updateWorkflowState(component, "foo", workflowState);
    verify(client).updateWorkflowState(component, "bar", workflowState);
    verify(cliOutput).printMessage("Workflow foo in component " + component + " disabled.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " disabled.");
  }

  @Test
  public void shouldHandleWorkflowNotFoundWhenEnabling() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .build();

    final ApiErrorException exception = new ApiErrorException("not found", 404, true);
    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    CliMain.run(cliContext, "workflow", "enable", component, "foo", "bar");
    verify(cliOutput).printMessage("Workflow foo in component " + component + " not found.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " not found.");
  }

  @Test
  public void shouldHandleWorkflowNotFoundWhenDisabling() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(false)
        .build();

    final ApiErrorException exception = new ApiErrorException("not found", 404, true);
    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    CliMain.run(cliContext, "workflow", "disable", component, "foo", "bar");
    verify(cliOutput).printMessage("Workflow foo in component " + component + " not found.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " not found.");
  }

  @Test
  public void testClientError() {
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
  public void testApiError() {
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
  public void testClientUnknownError() {
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
  public void testUnknownError() {
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
  public void testMissingCredentialsHelpMessage() {
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
  public void testUnauthorizedMessage() {
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
  public void testHelp() {
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
  public void testArgumentError() {
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
