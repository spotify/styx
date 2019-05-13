/*-
 * -\-\-
 * Spotify Styx CLI
 * --
 * Copyright (C) 2016 Spotify AB
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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.spotify.logging.LoggingConfigurator;
import com.spotify.logging.LoggingConfigurator.Level;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.TestServiceAccountUsageAuthorizationResponse;
import com.spotify.styx.cli.CliExitException.ExitStatus;
import com.spotify.styx.cli.CliMain.CliContext.Output;
import com.spotify.styx.client.ApiErrorException;
import com.spotify.styx.client.ClientErrorException;
import com.spotify.styx.client.StyxClient;
import com.spotify.styx.client.StyxClientFactory;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.BasicWorkflowValidator;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.ParameterUtil;
import com.spotify.styx.util.WorkflowValidator;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import javaslang.Tuple;
import javaslang.Tuple2;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

public final class CliMain {

  private static final String ENV_VAR_PREFIX = "STYX_CLI";

  private static final String COMMAND_DEST = "command";
  private static final String SUBCOMMAND_DEST = "subcommand";
  private static final String COMPONENT_DEST = "component";
  private static final String WORKFLOW_DEST = "workflow";
  private static final String PARAMETER_DEST = "parameter";

  private static final String STYX_CLI_VERSION =
      "Styx CLI " + CliMain.class.getPackage().getImplementationVersion();

  private final StyxCliParser parser;
  private final Namespace namespace;
  private final String apiHost;
  private final CliOutput cliOutput;
  private final CliContext cliContext;
  private final boolean debug;
  private StyxClient styxClient;

  private CliMain(
      StyxCliParser parser,
      Namespace namespace,
      String apiHost,
      CliOutput cliOutput,
      CliContext cliContext,
      boolean debug) {
    this.parser = Objects.requireNonNull(parser);
    this.namespace = Objects.requireNonNull(namespace);
    this.apiHost = Objects.requireNonNull(apiHost);
    this.cliOutput = Objects.requireNonNull(cliOutput);
    this.cliContext = Objects.requireNonNull(cliContext);
    this.debug = debug;
  }

  public static void main(String... args) {
    try {
      run(CliContext.DEFAULT, args);
    } catch (CliExitException e) {
      System.exit(e.status().code);
    }
  }

  static void run(CliContext cliContext, String... args) {
    run(cliContext, ImmutableList.copyOf(args));
  }

  static void run(CliContext cliContext, Collection<String> args) {
    final StyxCliParser parser = new StyxCliParser(cliContext);
    final Namespace namespace;
    final String apiHost;

    if (args.isEmpty()) {
      parser.parser.printHelp();
      throw CliExitException.of(ExitStatus.Success);
    }

    try {
      namespace = parser.parser.parseArgs(args.toArray(new String[0]));
      apiHost = namespace.getString(parser.globalOptions.host.getDest());
      if (apiHost == null) {
        throw new ArgumentParserException("Styx API host not set", parser.parser);
      }
    } catch (HelpScreenException e) {
      throw CliExitException.of(ExitStatus.Success);
    } catch (ArgumentParserException e) {
      parser.parser.handleError(e);
      throw CliExitException.of(ExitStatus.ArgumentError);
    }

    final boolean plainOutput = Objects.equals(namespace.getBoolean(parser.globalOptions.plain.getDest()), true);
    final boolean jsonOutput = Objects.equals(namespace.getBoolean(parser.globalOptions.json.getDest()), true);
    final CliOutput cliOutput;
    if (jsonOutput) {
      cliOutput = cliContext.output(Output.JSON);
    } else if (plainOutput) {
      cliOutput = cliContext.output(Output.PLAIN);
    } else {
      cliOutput = cliContext.output(Output.PRETTY);
    }

    final boolean debug = Objects.equals(namespace.getBoolean(parser.globalOptions.debug.getDest()), true);

    if (debug) {
      LoggingConfigurator.configureDefaults("styx-cli", Level.DEBUG);
    } else {
      LoggingConfigurator.configureNoLogging();
    }

    new CliMain(parser, namespace, apiHost, cliOutput, cliContext, debug).run();
  }

  private void run() {
    final Command command = namespace.get(COMMAND_DEST);

    try {
      styxClient = cliContext.createClient(apiHost);

      switch (command) {
        case LIST:
          activeStates();
          break;

        case EVENTS:
          eventsForWorkflowInstance();
          break;

        case TRIGGER:
          triggerWorkflowInstance();
          break;

        case HALT:
          haltWorkflowInstance();
          break;

        case RETRY:
          retryWorkflowInstance();
          break;

        case BACKFILL:
          final BackfillCommand backfillCommand = namespace.get(SUBCOMMAND_DEST);
          switch (backfillCommand) {
            case CREATE:
              backfillCreate();
              break;
            case EDIT:
              backfillEdit();
              break;
            case HALT:
              backfillHalt();
              break;
            case SHOW:
              backfillShow();
              break;
            case LIST:
              backfillList();
              break;
            default:
              // parsing unknown command will fail so this would only catch non-exhaustive switches
              throw new ArgumentParserException(
                  String.format("Unrecognized command: %s %s", command, backfillCommand),
                  parser.parser);
          }
          break;

        case RESOURCE:
          final ResourceCommand resourceCommand = namespace.get(SUBCOMMAND_DEST);
          switch (resourceCommand) {
            case CREATE:
              resourceCreate();
              break;
            case EDIT:
              resourceEdit();
              break;
            case SHOW:
              resourceShow();
              break;
            case LIST:
              resourceList();
              break;
            default:
              // parsing unknown command will fail so this would only catch non-exhaustive switches
              throw new ArgumentParserException(
                  String.format("Unrecognized command: %s %s", command, resourceCommand),
                  parser.parser);
          }
          break;

        case WORKFLOW:
          final WorkflowCommand workflowCommand = namespace.get(SUBCOMMAND_DEST);
          switch (workflowCommand) {
            case LIST:
              workflowList();
              break;
            case SHOW:
              workflowShow();
              break;
            case CREATE:
              workflowCreate();
              break;
            case DELETE:
              workflowDelete();
              break;
            case ENABLE:
              workflowEnable();
              break;
            case DISABLE:
              workflowDisable();
              break;
            default:
              // parsing unknown command will fail so this would only catch non-exhaustive switches
              throw new ArgumentParserException(
                  String.format("Unrecognized command: %s %s", command, workflowCommand),
                  parser.parser);
          }
          break;

        case AUTH:
          final AuthCommand authCommand = namespace.get(SUBCOMMAND_DEST);
          switch (authCommand) {
            case TEST:
              authTestServiceAccountUsage();
              break;
            default:
              // parsing unknown command will fail so this would only catch non-exhaustive switches
              throw new ArgumentParserException(
                  String.format("Unrecognized command: %s %s", command, authCommand),
                  parser.parser);
          }
          break;

        default:
          // parsing unknown command will fail so this would only catch non-exhaustive switches
          throw new ArgumentParserException("Unrecognized command: " + command, parser.parser);
      }
    } catch (ArgumentParserException e) {
      parser.parser.handleError(e);
      throw CliExitException.of(ExitStatus.ArgumentError);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (debug) {
        cliOutput.printError(getStackTraceAsString(cause));
      }
      if (cause instanceof ApiErrorException) {
        final ApiErrorException apiError = (ApiErrorException) cause;
        if (apiError.getCode() == HTTP_UNAUTHORIZED) {
          if (!apiError.isAuthenticated()) {
            cliOutput.printError(
                "API error: Unauthorized: " + apiError.getMessage() + "\n"
                    + "Please set up Application Default Credentials or set the "
                    + "GOOGLE_APPLICATION_CREDENTIALS env var to point to a file defining the "
                    + "credentials.\n"
                    + "\n"
                    + "Hint: Try setting up credentials using gcloud: \n"
                    + "\n"
                    + "\t$ gcloud auth application-default login");
          } else {
            cliOutput.printError("API error: Unauthorized: " + apiError.getMessage());
          }
          throw CliExitException.of(ExitStatus.AuthError);
        } else {
          cliOutput.printError("API error: " + apiError.getMessage());
          throw CliExitException.of(ExitStatus.ApiError);
        }
      } else if (cause instanceof ClientErrorException) {
        final Throwable rootCause = Throwables.getRootCause(cause);
        cliOutput.printError("Client error: " + cause.getMessage() + ": "
            + rootCause.getClass().getSimpleName() + ": " + rootCause.getMessage());
        throw CliExitException.of(ExitStatus.ClientError);
      } else {
        cliOutput.printError(getStackTraceAsString(cause));
        throw CliExitException.of(ExitStatus.ClientError);
      }
    } catch (CliExitException e) {
      throw e;
    } catch (Exception e) {
      cliOutput.printError(getStackTraceAsString(e));
      throw CliExitException.of(ExitStatus.UnknownError);
    } finally {
      styxClient.close();
    }
  }

  private void workflowDelete() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.workflowDeleteComponentId.getDest());
    final List<String> workflows = namespace.getList(parser.workflowDeleteWorkflowId.getDest());
    final boolean force = namespace.getBoolean(parser.workflowDeleteForce.getDest());

    // TODO: allow reading workflow ID's from file?

    if (cliContext.hasConsole() && !force) {
      final String reply = cliContext.consoleReadLine(
          "Sure you want to delete the workflow" + (workflows.size() > 1 ? "s" : "") + " "
              + workflows.stream().collect(joining(", "))
              + " in component " + component + "? [y/N] ").trim();
      if (!"y".equals(reply)) {
        throw CliExitException.of(ExitStatus.UnknownError);
      }
    }

    final List<Tuple2<String, CompletionStage<Void>>> futures = workflows.stream()
        .map(workflow -> Tuple.of(workflow, styxClient.deleteWorkflow(component, workflow)))
        .collect(toList());

    for (Tuple2<String, CompletionStage<Void>> future : futures) {
      final String workflow = future._1;
      try {
        future._2.toCompletableFuture().get();
        cliOutput.printMessage("Workflow " + workflow + " in component " + component + " deleted.");
      } catch (ExecutionException e) {
        handleWorkflowNotFound(component, workflow, e);
      }
    }
  }

  private void workflowCreate() throws IOException, ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.workflowCreateComponentId.getDest());
    final File file = namespace.get(parser.workflowCreateFile.getDest());

    final ObjectReader workflowReader = Json.YAML_MAPPER.reader()
        .forType(WorkflowConfiguration.class);
    final MappingIterator<WorkflowConfiguration> iterator;
    if (file == null || file.getName().equals("-")) {
      iterator = workflowReader.readValues(System.in);
    } else {
      iterator = workflowReader.readValues(file);
    }

    final List<WorkflowConfiguration> configurations = iterator.readAll();

    boolean invalid = false;
    for (WorkflowConfiguration configuration : configurations) {
      var workflow = Workflow.create(component, configuration);
      var errors = cliContext.workflowValidator().validateWorkflow(workflow);
      if (!errors.isEmpty()) {
        cliOutput.printError("Invalid workflow configuration: " + configuration.id());
        errors.forEach(error -> cliOutput.printError("  error: " + error));
        invalid = true;
      }
    }
    if (invalid) {
      throw CliExitException.of(ExitStatus.ArgumentError);
    }

    final List<CompletionStage<Workflow>> futures = configurations.stream()
        .map(configuration -> styxClient.createOrUpdateWorkflow(component, configuration))
        .collect(toList());

    for (CompletionStage<Workflow> future : futures) {
      final Workflow created = future.toCompletableFuture().get();
      cliOutput.printMessage("Workflow " + created.workflowId() + " in component "
          + created.componentId() + " created.");
    }
  }

  private void workflowEnable() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.workflowEnableComponentId.getDest());
    final List<String> workflows = namespace.getList(parser.workflowEnableWorkflowId.getDest());
    workflowStateUpdate(component, workflows, WorkflowState.patchEnabled(true))
        .forEach(t -> cliOutput
            .printMessage("Workflow " + t._1 + " in component " + component + " enabled."));
  }

  private void workflowDisable() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.workflowDisableComponentId.getDest());
    final List<String> workflows = namespace.getList(parser.workflowDisableWorkflowId.getDest());
    workflowStateUpdate(component, workflows, WorkflowState.patchEnabled(false))
        .forEach(t -> cliOutput
            .printMessage("Workflow " + t._1 + " in component " + component + " disabled."));
  }

  private List<Tuple2<String, WorkflowState>> workflowStateUpdate(String component,
                                                                  List<String> workflows,
                                                                  WorkflowState workflowState)
      throws ExecutionException, InterruptedException {
    final List<Tuple2<String, CompletionStage<WorkflowState>>> futures = workflows.stream()
        .map(workflow -> Tuple.of(workflow,
                                  styxClient.updateWorkflowState(component,
                                                                 workflow,
                                                                 workflowState)))
        .collect(toList());

    final List<Tuple2<String, WorkflowState>> updatedWorkflowStates =
        new ArrayList<>(futures.size());
    for (Tuple2<String, CompletionStage<WorkflowState>> future : futures) {
      final String workflow = future._1;
      try {
        updatedWorkflowStates.add(Tuple.of(workflow, future._2.toCompletableFuture().get()));
      } catch (ExecutionException e) {
        handleWorkflowNotFound(component, workflow, e);
      }
    }

    return updatedWorkflowStates;
  }

  private void handleWorkflowNotFound(String component, String workflow, ExecutionException e)
      throws ExecutionException {
    final Throwable cause = e.getCause();
    if (cause instanceof ApiErrorException) {
      final ApiErrorException apiError = (ApiErrorException) cause;
      if (apiError.getCode() == HTTP_NOT_FOUND) {
        cliOutput.printMessage("Workflow " + workflow + " in component " + component + " not found.");
      } else {
        throw e;
      }
    } else {
      throw e;
    }
  }

  private void backfillCreate() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.backfillCreateComponent.getDest());
    final String workflow = namespace.getString(parser.backfillCreateWorkflow.getDest());
    final String start = namespace.getString(parser.backfillCreateStart.getDest());
    final String end = namespace.getString(parser.backfillCreateEnd.getDest());
    final boolean reverse = namespace.getBoolean(parser.backfillCreateReverse.getDest());
    final int concurrency = namespace.getInt(parser.backfillCreateConcurrency.getDest());
    final String description = namespace.getString(parser.backfillCreateDescription.getDest());
    final TriggerParameters triggerParameters = TriggerParameters.builder()
        .env(parser.getEnvVars(namespace, parser.backfillCreateEnv))
        .build();
    final BackfillInput configuration = BackfillInput.newBuilder()
        .component(component)
        .workflow(workflow)
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .reverse(reverse)
        .concurrency(concurrency)
        .description(description)
        .triggerParameters(triggerParameters)
        .build();
    final boolean allowFuture = namespace.getBoolean(parser.backfillCreateAllowFuture.getDest());
    final Backfill backfill = styxClient.backfillCreate(configuration, allowFuture)
        .toCompletableFuture().get();
    cliOutput.printBackfill(backfill, true);
  }

  private void backfillEdit() throws ExecutionException, InterruptedException {
    final Integer concurrency = namespace.getInt(parser.backfillEditConcurrency.getDest());
    final String id = namespace.getString(parser.backfillEditId.getDest());

    if (concurrency != null) {
      final Backfill backfill =
          styxClient.backfillEditConcurrency(id, concurrency).toCompletableFuture().get();
      cliOutput.printBackfill(backfill, true);
    }
  }

  private void backfillHalt() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.backfillHaltId.getDest());

    styxClient.backfillHalt(id).toCompletableFuture().get();
    cliOutput.printMessage("Backfill halted! Use `styx backfill show " + id
                           + "` to check the backfill status.");
  }

  private void backfillShow() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.backfillShowId.getDest());
    final boolean noTruncate = namespace.getBoolean((parser.backfillShowNoTruncate.getDest()));

    final BackfillPayload backfillPayload = styxClient.backfill(id, true).toCompletableFuture().get();
    cliOutput.printBackfillPayload(backfillPayload, noTruncate);
  }

  private void backfillList() throws ExecutionException, InterruptedException {
    final Optional<String> component =
        Optional.ofNullable(namespace.getString(parser.backfillListComponent.getDest()));
    final Optional<String> workflow =
        Optional.ofNullable(namespace.getString(parser.backfillListWorkflow.getDest()));
    final boolean showAll = namespace.getBoolean(parser.backfillListShowAll.getDest());
    final boolean noTruncate = namespace.getBoolean(parser.backfillListNoTruncate.getDest());

    final BackfillsPayload backfillsPayload =
        styxClient.backfillList(component, workflow, showAll, false)
            .toCompletableFuture().get();
    cliOutput.printBackfills(backfillsPayload.backfills(), noTruncate);
  }

  private void resourceCreate() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.resourceCreateId.getDest());
    final int concurrency = namespace.getInt(parser.resourceCreateConcurrency.getDest());

    final Resource resource =
        styxClient.resourceCreate(id, concurrency).toCompletableFuture().get();
    cliOutput.printResources(Collections.singletonList(resource));
  }

  private void resourceEdit() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.resourceEditId.getDest());
    final Integer concurrency = namespace.getInt(parser.resourceEditConcurrency.getDest());

    if (concurrency != null) {
      Resource resource = styxClient.resourceEdit(id, concurrency).toCompletableFuture().get();
      cliOutput.printResources(Collections.singletonList(resource));
    }
  }

  private void resourceShow() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.resourceShowId.getDest());

    final Resource resource = styxClient.resource(id).toCompletableFuture().get();
    cliOutput.printResources(Collections.singletonList(resource));
  }

  private void resourceList() throws ExecutionException, InterruptedException {
    final ResourcesPayload resourcesPayload =
        styxClient.resourceList().toCompletableFuture().get();
    cliOutput.printResources(resourcesPayload.resources());
  }

  private void workflowShow() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(parser.workflowShowComponentId.getDest());
    final String workflow = namespace.getString(parser.workflowShowWorkflowId.getDest());

    final CompletableFuture<Workflow> workflowFuture =
        styxClient.workflow(component, workflow).toCompletableFuture();
    final CompletableFuture<WorkflowState> workflowStateFuture =
        styxClient.workflowState(component, workflow).toCompletableFuture();

    final Tuple2<Workflow, WorkflowState> tuple =
        workflowFuture.thenCombine(workflowStateFuture, Tuple::of).toCompletableFuture().get();
    cliOutput.printWorkflow(tuple._1, tuple._2);
  }

  private void workflowList() throws ExecutionException, InterruptedException {
    cliOutput.printWorkflows(styxClient.workflows().toCompletableFuture().get());
  }

  private void activeStates() throws ExecutionException, InterruptedException {
    final Optional<String> component =
        Optional.ofNullable(namespace.getString(parser.listComponent.getDest()));

    final RunStateDataPayload runStateDataPayload =
        styxClient.activeStates(component).toCompletableFuture().get();
    cliOutput.printStates(runStateDataPayload);
  }

  private void eventsForWorkflowInstance() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(COMPONENT_DEST);
    final String workflow = namespace.getString(WORKFLOW_DEST);
    final String parameter = namespace.getString(PARAMETER_DEST);

    final List<EventInfo> eventInfos =
        styxClient.eventsForWorkflowInstance(component, workflow, parameter)
            .toCompletableFuture().get();
    cliOutput.printEvents(eventInfos);
  }

  private void triggerWorkflowInstance() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(COMPONENT_DEST);
    final String workflow = namespace.getString(WORKFLOW_DEST);
    final String parameter = namespace.getString(PARAMETER_DEST);
    final TriggerParameters parameters = TriggerParameters.builder()
        .env(parser.getEnvVars(namespace, parser.triggerEnv))
        .build();
    final boolean allowFuture = namespace.getBoolean(parser.triggerAllowFuture.getDest());

    styxClient.triggerWorkflowInstance(component, workflow, parameter, parameters, allowFuture)
        .toCompletableFuture().get();
    cliOutput.printMessage("Triggered! Use `styx ls -c " + component
                           + "` to check active workflow instances.");
  }

  private void haltWorkflowInstance() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(COMPONENT_DEST);
    final String workflow = namespace.getString(WORKFLOW_DEST);
    final String parameter = namespace.getString(PARAMETER_DEST);

    styxClient.haltWorkflowInstance(component, workflow, parameter).toCompletableFuture().get();
    cliOutput.printMessage("Halted! Use `styx events " + component + " " + workflow + " "
                           + parameter + "` to verify.");
  }

  private void retryWorkflowInstance() throws ExecutionException, InterruptedException {
    final String component = namespace.getString(COMPONENT_DEST);
    final String workflow = namespace.getString(WORKFLOW_DEST);
    final String parameter = namespace.getString(PARAMETER_DEST);

    styxClient.retryWorkflowInstance(component, workflow, parameter).toCompletableFuture().get();
    cliOutput.printMessage("Retrying! Use `styx ls -c " + component
                           + "` to check active workflow instances.");
  }

  private void authTestServiceAccountUsage() throws ExecutionException, InterruptedException {
    final String serviceAccount = namespace.getString(parser.authServiceAccount.getDest());
    final String principal = namespace.getString(parser.authPrincipal.getDest());

    final TestServiceAccountUsageAuthorizationResponse response = styxClient.testServiceAccountUsageAuthorization(
        serviceAccount, principal).toCompletableFuture().get();

    if (response.authorized()) {
      cliOutput.printMessage("The principal " + principal + " is authorized to use the service account "
                             + serviceAccount + ". " + response.message().orElse(""));
    } else {
      cliOutput.printMessage("The principal " + principal + " is not authorized to use the service account "
                             + serviceAccount + ". " + response.message().orElse(""));
      throw CliExitException.of(ExitStatus.UnknownError);
    }
  }

  private static class ParserBase {
    final CliContext cliContext;

    protected ParserBase(CliContext cliContext) {
      this.cliContext = cliContext;
    }
  }

  private static class StyxCliParser extends ParserBase {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("styx")
        .description("Styx CLI")
        .version(STYX_CLI_VERSION);

    final PartitionAction partitionAction = new PartitionAction();

    final Subparsers subCommands = parser.addSubparsers().title("commands").metavar(" ");

    final Subparsers backfillParser =
        Command.BACKFILL.parser(subCommands, cliContext)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser backfillShow = BackfillCommand.SHOW.parser(backfillParser, cliContext);
    final Argument backfillShowId =
        backfillShow.addArgument("backfill").help("Backfill ID");
    final Argument backfillShowNoTruncate = backfillShow
        .addArgument("--no-trunc")
        .setDefault(false)
        .action(Arguments.storeTrue())
        .help("don't truncate output (only for pretty printing)");

    final Subparser backfillEdit = BackfillCommand.EDIT.parser(backfillParser, cliContext);
    final Argument backfillEditId =
        backfillEdit.addArgument("backfill").help("Backfill ID");
    final Argument backfillEditConcurrency =
        backfillEdit.addArgument("--concurrency").help("set the concurrency value for the backfill")
            .type(Integer.class);

    final Subparser backfillHalt = BackfillCommand.HALT.parser(backfillParser, cliContext);
    final Argument backfillHaltId =
        backfillHalt.addArgument("backfill").help("Backfill ID");

    final Subparser backfillList = BackfillCommand.LIST.parser(backfillParser, cliContext);
    final Argument backfillListWorkflow =
        backfillList.addArgument("-w", "--workflow").help("only show backfills for WORKFLOW");
    final Argument backfillListComponent =
        backfillList.addArgument("-c", "--component").help("only show backfills for COMPONENT");
    final Argument backfillListShowAll =
        backfillList.addArgument("-a", "--show-all")
            .setDefault(false)
            .action(Arguments.storeTrue())
            .help("show all backfills, even halted and all-triggered ones");
    final Argument backfillListNoTruncate = backfillList
            .addArgument("--no-trunc")
            .setDefault(false)
            .action(Arguments.storeTrue())
            .help("don't truncate output (only for pretty printing)");

    final Subparser backfillCreate = BackfillCommand.CREATE.parser(backfillParser, cliContext);
    final Argument backfillCreateComponent =
        backfillCreate.addArgument("component").help("Component ID");
    final Argument backfillCreateWorkflow =
        backfillCreate.addArgument("workflow").help("Workflow ID");
    final Argument backfillCreateStart =
        backfillCreate.addArgument("start").help("Start date/datehour (inclusive)").action(partitionAction);
    final Argument backfillCreateEnd =
        backfillCreate.addArgument("end").help("End date/datehour (exclusive)").action(partitionAction);
    final Argument backfillCreateReverse =
        backfillCreate.addArgument("--reverse")
            .setDefault(false)
            .help("Run backfill in reverse, from end (exclusive) to beginning (inclusive)")
            .action(Arguments.storeTrue());
    final Argument backfillCreateConcurrency =
        backfillCreate.addArgument("concurrency").help("The number of jobs to run in parallel").type(Integer.class);
    final Argument backfillCreateDescription =
        backfillCreate.addArgument("-d", "--description")
            .help("a description of the backfill");
    final Argument backfillCreateEnv = addEnvVarArgument(backfillCreate, "-e", "--env");
    final Argument backfillCreateAllowFuture = addEnvVarArgument(backfillCreate, "--allow-future")
        .help("Allow backfilling future partitions")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Subparsers resourceParser =
        Command.RESOURCE.parser(subCommands, cliContext)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser resourceShow = ResourceCommand.SHOW.parser(resourceParser, cliContext);
    final Argument resourceShowId =
        resourceShow.addArgument("id").help("Resource ID");

    final Subparser resourceEdit = ResourceCommand.EDIT.parser(resourceParser, cliContext);
    final Argument resourceEditId =
        resourceEdit.addArgument("id").help("Resource ID");
    final Argument resourceEditConcurrency =
        resourceEdit.addArgument("--concurrency")
            .help("set the concurrency value for the resource")
            .type(Integer.class);

    final Subparser resourceList = ResourceCommand.LIST.parser(resourceParser, cliContext);

    final Subparser resourceCreate = ResourceCommand.CREATE.parser(resourceParser, cliContext);
    final Argument resourceCreateId =
        resourceCreate.addArgument("id").help("Resource ID");
    final Argument resourceCreateConcurrency =
        resourceCreate.addArgument("concurrency")
            .help("The concurrency of this resource")
            .type(Integer.class);

    final Subparsers workflowParser =
        Command.WORKFLOW.parser(subCommands, cliContext)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser workflowList = WorkflowCommand.LIST.parser(workflowParser, cliContext);

    final Subparser workflowShow = WorkflowCommand.SHOW.parser(workflowParser, cliContext);
    final Argument workflowShowComponentId =
        workflowShow.addArgument("component").help("Component ID");
    final Argument workflowShowWorkflowId =
        workflowShow.addArgument("workflow").help("Workflow ID");

    final Subparser workflowCreate = WorkflowCommand.CREATE.parser(workflowParser, cliContext);
    final Argument workflowCreateComponentId =
        workflowCreate.addArgument("component").help("Component ID");
    final Argument workflowCreateFile =
        workflowCreate.addArgument("-f", "--file")
            .type(fileType().acceptSystemIn().verifyCanRead())
            .help("Workflow configuration file");

    final Subparser workflowDelete = WorkflowCommand.DELETE.parser(workflowParser, cliContext);
    final Argument workflowDeleteComponentId =
        workflowDelete.addArgument("component").help("Component ID");
    final Argument workflowDeleteWorkflowId =
        workflowDelete.addArgument("workflow").nargs("+").help("Workflow IDs");
    final Argument workflowDeleteForce =
        workflowDelete.addArgument("--force")
            .help("Do not ask for confirmation")
            .setDefault(false)
            .action(Arguments.storeTrue());

    final Subparser workflowEnable = WorkflowCommand.ENABLE.parser(workflowParser, cliContext);
    final Argument workflowEnableComponentId =
        workflowEnable.addArgument("component").help("Component ID");
    final Argument workflowEnableWorkflowId =
        workflowEnable.addArgument("workflow").nargs("+").help("Workflow IDs");

    final Subparser workflowDisable = WorkflowCommand.DISABLE.parser(workflowParser, cliContext);
    final Argument workflowDisableComponentId =
        workflowDisable.addArgument("component").help("Component ID");
    final Argument workflowDisableWorkflowId =
        workflowDisable.addArgument("workflow").nargs("+").help("Workflow IDs");

    final Subparser list = Command.LIST.parser(subCommands, cliContext);
    final Argument listComponent = list.addArgument("-c", "--component")
        .help("only show instances for COMPONENT");

    final Subparser events = addWorkflowInstanceArguments(Command.EVENTS.parser(subCommands, cliContext));
    final Subparser trigger = addWorkflowInstanceArguments(Command.TRIGGER.parser(subCommands, cliContext));
    final Argument triggerEnv = addEnvVarArgument(trigger, "-e", "--env");
    final Argument triggerAllowFuture = addEnvVarArgument(trigger, "--allow-future")
        .help("Allow triggering future partition")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Subparser halt = addWorkflowInstanceArguments(Command.HALT.parser(subCommands, cliContext));
    final Subparser retry = addWorkflowInstanceArguments(Command.RETRY.parser(subCommands, cliContext));

    final Subparsers authParser =
        Command.AUTH.parser(subCommands, cliContext)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser testServiceAccountUsage = AuthCommand.TEST.parser(authParser, cliContext);
    final Argument authServiceAccount = testServiceAccountUsage
        .addArgument("-a", "--service-account")
        .required(true)
        .help("The service account to test usage authorization against");
    final Argument authPrincipal = testServiceAccountUsage
        .addArgument("-u", "--principal")
        .required(true)
        .help("The principal (user) to test for service account usage authorization");

    final GlobalOptions globalOptions = GlobalOptions.add(parser, cliContext, false);

    final Argument version = parser.addArgument("--version").action(Arguments.version());

    private StyxCliParser(CliContext cliContext) {
      super(cliContext);
    }

    private static Subparser addWorkflowInstanceArguments(Subparser subparser) {
      subparser.addArgument(COMPONENT_DEST)
          .help("Component id");
      subparser.addArgument(WORKFLOW_DEST)
          .help("Workflow id");
      subparser.addArgument(PARAMETER_DEST)
          .help("Parameter identifying the workflow instance, e.g. '2016-09-14' or '2016-09-14T17'");
      return subparser;
    }

    private Argument addEnvVarArgument(ArgumentParser parser, String... flags) {
      return parser.addArgument(flags).type(String.class)
          .action(append())
          .help("Environment variables");
    }

    private Map<String, String> getEnvVars(Namespace namespace, Argument argument) {
      final List<String> args = namespace.getList(argument.getDest());
      return args == null
          ? Collections.emptyMap()
          : args.stream()
              .map(s -> Splitter.on('=').splitToList(s))
              .peek(kv -> Preconditions.checkArgument(kv.size() == 2))
              .collect(toMap(kv -> kv.get(0), kv -> kv.get(1)));
    }
  }

  private enum Command {
    LIST("ls", "List active workflow instances"),
    EVENTS("e", "List events for a workflow instance"),
    HALT("h", "Halt a workflow instance"),
    TRIGGER("t", "Trigger a workflow instance, fail if the instance is already active"),
    RETRY("r", "Retry a workflow instance that is in a waiting state"),
    RESOURCE(null, "Commands related to resources"),
    BACKFILL(null, "Commands related to backfills"),
    WORKFLOW(null, "Commands related to workflows"),
    AUTH(null, "Commands related to authentication and authorization");

    private final String alias;
    private final String description;

    Command(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands, CliContext cliContext) {
      final Subparser subparser = addSubparser(subCommands, name().toLowerCase(), cliContext)
          .setDefault(COMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }

  }

  private static Subparser addSubparser(Subparsers subCommands, String name, CliContext cliContext) {
    final Subparser parser = subCommands.addParser(name);
    GlobalOptions.add(parser, cliContext, true);
    return parser;
  }

  private static class GlobalOptions {

    final Argument host;
    final Argument json;
    final Argument plain;
    final Argument debug;
    final ArgumentGroup options;

    private GlobalOptions(ArgumentParser parser, CliContext cliContext, boolean subCommand) {
      this.options = parser.addArgumentGroup("global options");
      this.host = options.addArgument("-H", "--host")
          .help("Styx API host (can also be set with environment variable " + ENV_VAR_PREFIX + "_HOST)")
          .setDefault(subCommand ? FeatureControl.SUPPRESS : null)
          .setDefault(cliContext.env().get(ENV_VAR_PREFIX + "_HOST"))
          .action(Arguments.store());
      this.json = options.addArgument("--json")
          .help("json output")
          .setDefault(subCommand ? FeatureControl.SUPPRESS : null)
          .action(Arguments.storeTrue());
      this.plain = options.addArgument("-p", "--plain")
          .help("plain output")
          .setDefault(subCommand ? FeatureControl.SUPPRESS : null)
          .action(Arguments.storeTrue());
      this.debug = options.addArgument("--debug")
          .help("debug output")
          .setDefault(subCommand ? FeatureControl.SUPPRESS : null)
          .action(Arguments.storeTrue());
    }

    static GlobalOptions add(ArgumentParser parser, CliContext cliContext, boolean subCommand) {
      return new GlobalOptions(parser, cliContext, subCommand);
    }
  }

  private enum BackfillCommand {
    LIST("ls", "List active backfills. Use option -a (--show-all) to show all"),
    CREATE("", "Create a backfill"),
    EDIT("e", "Edit a backfill"),
    HALT("h", "Halt a backfill"),
    SHOW("get", "Show info about a specific backfill");

    private final String alias;
    private final String description;

    BackfillCommand(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands, CliContext cliContext) {
      final Subparser subparser = addSubparser(subCommands, name().toLowerCase(), cliContext)
          .setDefault(SUBCOMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  private enum ResourceCommand {
    LIST("ls", "List resources"),
    CREATE("", "Create a resource"),
    EDIT("e", "Edit a resource"),
    SHOW("get", "Show info about a specific resource");

    private final String alias;
    private final String description;

    ResourceCommand(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands, CliContext cliContext) {
      final Subparser subparser = addSubparser(subCommands, name().toLowerCase(), cliContext)
          .setDefault(SUBCOMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  private enum WorkflowCommand {
    LIST("ls", "List all workflows"),
    SHOW("get", "Show info about a specific workflow"),
    CREATE("", "Create or update workflow(s)"),
    DELETE("", "Delete workflow(s)"),
    ENABLE("", "Enable workflow(s)"),
    DISABLE("", "Disable workflow(s)");

    private final String alias;
    private final String description;

    WorkflowCommand(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands, CliContext cliContext) {
      final Subparser subparser = addSubparser(subCommands, name().toLowerCase(), cliContext)
          .setDefault(SUBCOMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  private enum AuthCommand {
    TEST("", "Test authorization");

    private final String alias;
    private final String description;

    AuthCommand(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands, CliContext cliContext) {
      final Subparser subparser = addSubparser(subCommands, name().toLowerCase(), cliContext)
          .setDefault(SUBCOMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  private static class PartitionAction implements ArgumentAction {

    @Override
    public void run(ArgumentParser parser, Argument arg,
                    Map<String, Object> attrs, String flag, Object value)
        throws ArgumentParserException {
      try {
        attrs.put(arg.getDest(),
                  ParameterUtil.parseDateHour(value.toString()));
      } catch (DateTimeParseException dateHourException) {
        try {
          attrs.put(arg.getDest(),
                    ParameterUtil.parseDate(value.toString()));
        } catch (Exception dateException) {
          throw new ArgumentParserException(
              String.format(
                  "could not parse date/datehour for parameter '%s'; if datehour: [%s], if date: [%s]",
                  arg.textualName(), dateHourException.getMessage(), dateException.getMessage()),
              parser);
        }
      }
    }

    @Override
    public void onAttach(Argument arg) {
      // nop
    }

    @Override
    public boolean consumeArgument() {
      return true;
    }
  }

  interface CliContext {

    enum Output {
      JSON,
      PLAIN,
      PRETTY
    }

    CliOutput output(Output output);

    Map<String, String> env();

    StyxClient createClient(String host);

    boolean hasConsole();

    String consoleReadLine(String prompt);

    WorkflowValidator workflowValidator();

    CliContext DEFAULT = new CliContext() {
      @Override
      public StyxClient createClient(String host) {
        return StyxClientFactory.create(host);
      }

      @Override
      public CliOutput output(Output output) {
        switch (output) {
          case JSON:
            return new JsonCliOutput();
          case PLAIN:
            return new PlainCliOutput();
          case PRETTY:
            return new PrettyCliOutput();
          default:
            throw new AssertionError();
        }
      }

      @Override
      public Map<String, String> env() {
        return System.getenv();
      }

      @Override
      public boolean hasConsole() {
        return System.console() != null;
      }

      @Override
      public String consoleReadLine(String prompt) {
        if (!hasConsole()) {
          throw new IllegalStateException();
        }
        return System.console().readLine(prompt);
      }

      @Override
      public WorkflowValidator workflowValidator() {
        return new BasicWorkflowValidator(new DockerImageValidator());
      }
    };
  }
}
