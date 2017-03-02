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

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.deserialize;
import static com.spotify.styx.serialization.Json.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.core.Services;
import com.spotify.apollo.environment.ApolloEnvironmentModule;
import com.spotify.apollo.http.client.HttpClientModule;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.ParameterUtil;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import okio.ByteString;

public final class Main {

  private static final String UTF_8 = "UTF-8";
  private static final String ENV_VAR_PREFIX = "STYX_CLI";
  private static final String STYX_API_ENDPOINT = "/api/v1";
  private static final int TTL_REQUEST = 90;

  private static final String COMMAND_DEST = "command";
  private static final String SUBCOMMAND_DEST = "subcommand";
  private static final String COMPONENT_DEST = "component";
  private static final String WORKFLOW_DEST = "workflow";
  private static final String PARAMETER_DEST = "parameter";

  private static final int EXIT_CODE_SUCCESS = 0;
  private static final int EXIT_CODE_UNKNOWN_ERROR = 1;
  private static final int EXIT_CODE_ARGUMENT_ERROR = 2;
  private static final int EXIT_CODE_API_ERROR = 3;

  private final StyxCliParser parser;
  private final Namespace namespace;
  private final String apiHost;
  private final Service cliService;
  private final CliOutput cliOutput;
  private Client client;

  private Main(
      StyxCliParser parser,
      Namespace namespace,
      String apiHost,
      Service cliService,
      CliOutput cliOutput) {
    this.parser = Objects.requireNonNull(parser);
    this.namespace = Objects.requireNonNull(namespace);
    this.apiHost = Objects.requireNonNull(apiHost);
    this.cliService = Objects.requireNonNull(cliService);
    this.cliOutput = Objects.requireNonNull(cliOutput);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final StyxCliParser parser = new StyxCliParser();
    final Namespace namespace;
    final String apiHost;

    try {
      namespace = parser.parser.parseArgs(args);
      apiHost = namespace.getString(parser.host.getDest());
      if (apiHost == null) {
        throw new ArgumentParserException("Styx API host not set", parser.parser);
      }
    } catch (HelpScreenException e) {
      System.exit(EXIT_CODE_SUCCESS);
      return; // needed to convince compiler that flow is interrupted
    } catch (ArgumentParserException e) {
      parser.parser.handleError(e);
      System.exit(EXIT_CODE_ARGUMENT_ERROR);
      return; // needed to convince compiler that flow is interrupted
    }

    final Service cliService = Services.usingName("styx-cli")
        .withEnvVarPrefix(ENV_VAR_PREFIX)
        .withModule(ApolloEnvironmentModule.create())
        .withModule(HttpClientModule.create())
        .build();

    final boolean plainOutput = namespace.getBoolean(parser.plain.getDest());
    final CliOutput cliOutput = plainOutput ? new PlainCliOutput() : new PrettyCliOutput();

    new Main(parser, namespace, apiHost, cliService, cliOutput).run();
  }

  private void run() {
    final Command command = namespace.get(COMMAND_DEST);

    try (Service.Instance instance = cliService.start()) {
      client = ApolloEnvironmentModule.environment(instance).environment().client();

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

        default:
          // parsing unknown command will fail so this would only catch non-exhaustive switches
          throw new ArgumentParserException("Unrecognized command: " + command, parser.parser);
      }
    } catch (ArgumentParserException e) {
      parser.parser.handleError(e);
      System.exit(EXIT_CODE_ARGUMENT_ERROR);
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      System.exit(EXIT_CODE_UNKNOWN_ERROR);
    } catch (Exception e) {
      System.err.println(e.getClass().getSimpleName() + ": " + e.getMessage());
      System.exit(EXIT_CODE_API_ERROR);
    }
  }

  private void backfillCreate() throws ExecutionException, InterruptedException, IOException {
    final String component = namespace.getString(parser.backfillCreateComponent.getDest());
    final String workflow = namespace.getString(parser.backfillCreateWorkflow.getDest());
    final String start = namespace.getString(parser.backfillCreateStart.getDest());
    final String end = namespace.getString(parser.backfillCreateEnd.getDest());
    final int concurrency = namespace.getInt(parser.backfillCreateConcurrency.getDest());

    final BackfillInput backfillInput = BackfillInput.create(
        Instant.parse(start), Instant.parse(end), component, workflow, concurrency);

    final ByteString payload = serialize(backfillInput);
    final ByteString response = send(
        Request.forUri(apiUrl("backfills"), "POST").withPayload(payload));
    final Backfill backfill = deserialize(response, Backfill.class);
    cliOutput.printBackfill(backfill);
  }

  private void backfillEdit() throws ExecutionException, InterruptedException, IOException {
    final Integer concurrency = namespace.getInt(parser.backfillEditConcurrency.getDest());
    final String id = namespace.getString(parser.backfillEditId.getDest());

    final ByteString getResponse = send(Request.forUri(apiUrl("backfills", id)));
    final BackfillPayload backfillPayload = deserialize(getResponse, BackfillPayload.class);
    final BackfillBuilder editedBackfillBuilder = backfillPayload.backfill().builder();
    if (concurrency != null) {
      editedBackfillBuilder.concurrency(concurrency);
    }
    final ByteString putPayload = serialize(editedBackfillBuilder.build());
    final ByteString putResponse = send(
        Request.forUri(apiUrl("backfills", id), "PUT").withPayload(putPayload));

    final Backfill newBackfill = deserialize(putResponse, Backfill.class);
    cliOutput.printBackfill(newBackfill);
  }

  private void backfillHalt() throws ExecutionException, InterruptedException {
    final String id = namespace.getString(parser.backfillHaltId.getDest());
    send(Request.forUri(apiUrl("backfills", id), "DELETE"));
  }

  private void backfillShow() throws ExecutionException, InterruptedException, IOException {
    final String id = namespace.getString(parser.backfillShowId.getDest());
    final ByteString response = send(Request.forUri(apiUrl("backfills", id)));
    final BackfillPayload backfillStatus = deserialize(response, BackfillPayload.class);
    cliOutput.printBackfillPayload(backfillStatus);
  }

  private void backfillList()
      throws IOException, ExecutionException, InterruptedException {
    String uri = apiUrl("backfills");
    final List<String> queries = new ArrayList<>();
    final String component = namespace.getString(parser.backfillListComponent.getDest());
    if (component != null) {
      queries.add("component=" + URLEncoder.encode(component, UTF_8));
    }
    final String workflow = namespace.getString(parser.backfillListWorkflow.getDest());
    if (workflow != null) {
      queries.add("workflow=" + URLEncoder.encode(workflow, UTF_8));
    }
    if (!queries.isEmpty()) {
      uri += "?" + String.join("&", queries);
    }
    final ByteString response = send(Request.forUri(uri));
    final BackfillsPayload backfills = deserialize(response, BackfillsPayload.class);
    cliOutput.printBackfills(backfills.backfills());
  }

  private void activeStates() throws IOException, ExecutionException, InterruptedException {
    String uri = apiUrl("cli", "activeStates");
    final String component = namespace.getString(parser.listComponent.getDest());
    if (component != null) {
      uri += "?component=" + URLEncoder.encode(component, UTF_8);
    }

    final ByteString response = send(Request.forUri(uri).withTtl(Duration.ofSeconds(TTL_REQUEST)));
    final RunStateDataPayload runStateDataPayload = deserialize(response, RunStateDataPayload.class);
    cliOutput.printStates(runStateDataPayload);
  }

  private void eventsForWorkflowInstance()
      throws ExecutionException, InterruptedException, IOException {
    final WorkflowInstance workflowInstance = getWorkflowInstance(namespace);
    final String component = workflowInstance.workflowId().componentId();
    final String workflow = workflowInstance.workflowId().endpointId();
    final String parameter = workflowInstance.parameter();

    final ByteString response = send(
        Request.forUri(apiUrl("cli", "events", component, workflow, parameter))
            .withTtl(Duration.ofSeconds(TTL_REQUEST)));

    final JsonNode jsonNode = OBJECT_MAPPER.readTree(response.toByteArray());

    if (!jsonNode.isObject()) {
      throw new RuntimeException("Invalid json returned from API");
    }

    final ObjectNode json = (ObjectNode) jsonNode;
    final ArrayNode events = json.withArray("events");
    final ImmutableList.Builder<CliOutput.EventInfo> eventInfos = ImmutableList.builder();
    for (JsonNode eventWithTimestamp : events) {
      final long ts = eventWithTimestamp.get("timestamp").asLong();
      final JsonNode event = eventWithTimestamp.get("event");

      String eventName;
      String eventInfo;
      try {
        Event typedEvent = OBJECT_MAPPER.convertValue(event, Event.class);
        eventName = EventUtil.name(typedEvent);
        eventInfo = CliUtil.info(typedEvent);
      } catch (IllegalArgumentException e) {
        // fall back to just inspecting the json
        eventName = event.get("@type").asText();
        eventInfo = "";
      }

      eventInfos.add(CliOutput.EventInfo.create(ts, eventName, eventInfo));
    }

    cliOutput.printEvents(eventInfos.build());
  }

  private void triggerWorkflowInstance()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    final WorkflowInstance workflowInstance = getWorkflowInstance(namespace);
    final ByteString payload = serialize(workflowInstance);
    send(Request.forUri(apiUrl("cli", "trigger"), "POST").withPayload(payload));
  }

  private void haltWorkflowInstance()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    final WorkflowInstance workflowInstance = getWorkflowInstance(namespace);
    final ByteString payload = serialize(Event.halt(workflowInstance));
    send(Request.forUri(apiUrl("cli", "events"), "POST").withPayload(payload));
  }

  private void retryWorkflowInstance()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    final WorkflowInstance workflowInstance = getWorkflowInstance(namespace);
    final ByteString payload = serialize(Event.dequeue(workflowInstance));
    send(Request.forUri(apiUrl("cli", "events"), "POST").withPayload(payload));
  }

  private static WorkflowInstance getWorkflowInstance(Namespace namespace) {
    return WorkflowInstance.create(
        WorkflowId.create(
            namespace.getString(COMPONENT_DEST),
            namespace.getString(WORKFLOW_DEST)),
        namespace.getString(PARAMETER_DEST));
  }

  private String apiUrl(CharSequence... parts) {
    return "http://" + apiHost + STYX_API_ENDPOINT + "/" + String.join("/", parts);
  }

  private ByteString send(Request request) throws ExecutionException, InterruptedException {
    final Response<ByteString> response = client.send(request).toCompletableFuture().get();

    switch (response.status().family()) {
      case SUCCESSFUL:
        return response.payload().orElse(ByteString.EMPTY);
      default:
        throw new RuntimeException(
            String.format("API error: %d %s",
                          response.status().code(),
                          response.status().reasonPhrase()));
    }
  }

  private static class StyxCliParser {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("styx")
        .description("Styx CLI")
        .version("Styx CLI " + Main.class.getPackage().getImplementationVersion());

    final PartitionAction partitionAction = new PartitionAction();

    final Subparsers subCommands = parser.addSubparsers().title("commands").metavar(" ");

    final Subparsers backfillParser =
        Command.BACKFILL.parser(subCommands)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser backfillShow = BackfillCommand.SHOW.parser(backfillParser);
    final Argument backfillShowId =
        backfillShow.addArgument("backfill").help("Backfill ID");

    final Subparser backfillEdit = BackfillCommand.EDIT.parser(backfillParser);
    final Argument backfillEditId =
        backfillEdit.addArgument("backfill").help("Backfill ID");
    final Argument backfillEditConcurrency =
        backfillEdit.addArgument("--concurrency").help("set the concurrency value for the backfill")
            .type(Integer.class);

    final Subparser backfillHalt = BackfillCommand.HALT.parser(backfillParser);
    final Argument backfillHaltId =
        backfillHalt.addArgument("backfill").help("Backfill ID");

    final Subparser backfillList = BackfillCommand.LIST.parser(backfillParser);
    final Argument backfillListWorkflow =
        backfillList.addArgument("-w", "--workflow").help("only show  backfills for WORKFLOW");
    final Argument backfillListComponent =
        backfillList.addArgument("-c", "--component").help("only show  backfills for COMPONENT");

    final Subparser backfillCreate = BackfillCommand.CREATE.parser(backfillParser);
    final Argument backfillCreateComponent =
        backfillCreate.addArgument(COMPONENT_DEST).help("Component ID");
    final Argument backfillCreateWorkflow =
        backfillCreate.addArgument(WORKFLOW_DEST).help("Workflow ID");
    final Argument backfillCreateStart =
        backfillCreate.addArgument("start").help("Start date/datehour (inclusive)").action(partitionAction);
    final Argument backfillCreateEnd =
        backfillCreate.addArgument("end").help("End date/datehour (exclusive)").action(partitionAction);
    final Argument backfillCreateConcurrency =
        backfillCreate.addArgument("concurrency").help("The number of jobs to run in parallel").type(Integer.class);

    final Subparser list = Command.LIST.parser(subCommands);
    final Argument listComponent = list.addArgument("-c", "--component")
        .help("only show instances for COMPONENT");

    final Subparser events = addWorkflowInstanceArguments(Command.EVENTS.parser(subCommands));
    final Subparser trigger = addWorkflowInstanceArguments(Command.TRIGGER.parser(subCommands));
    final Subparser halt = addWorkflowInstanceArguments(Command.HALT.parser(subCommands));
    final Subparser retry = addWorkflowInstanceArguments(Command.RETRY.parser(subCommands));

    final Argument host = parser.addArgument("-H", "--host")
        .help("Styx API host (can also be set with environment variable " + ENV_VAR_PREFIX + "_HOST)")
        .setDefault(System.getenv(ENV_VAR_PREFIX + "_HOST"))
        .action(Arguments.store());

    final Argument plain = parser.addArgument("-p", "--plain")
        .help("plain output")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Argument version = parser.addArgument("--version").action(Arguments.version());

    private static Subparser addWorkflowInstanceArguments(Subparser subparser) {
      subparser.addArgument(COMPONENT_DEST)
          .help("Component id");
      subparser.addArgument(WORKFLOW_DEST)
          .help("Workflow id (legacy Endpoint)");
      subparser.addArgument(PARAMETER_DEST)
          .help("Parameter identifying the workflow instance, e.g. '2016-09-14' or '2016-09-14T17'");
      return subparser;
    }
  }

  private enum Command {
    LIST("ls", "List active workflow instances"),
    EVENTS("e", "List events for a workflow instance"),
    HALT("h", "Halt a workflow instance"),
    TRIGGER("t", "Trigger a completed workflow instance"),
    RETRY("r", "Retry a workflow instance that is in a waiting state"),
    BACKFILL(null, "Commands related to backfills");

    private final String alias;
    private final String description;

    Command(String alias, String description) {
      this.alias = alias;
      this.description = description;
    }

    public Subparser parser(Subparsers subCommands) {
      Subparser subparser = subCommands
          .addParser(name().toLowerCase())
          .setDefault(COMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  private enum BackfillCommand {
    LIST("ls", "List backfills"),
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

    public Subparser parser(Subparsers subCommands) {
      Subparser subparser = subCommands
          .addParser(name().toLowerCase())
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
      Instant instant = null;
      try {
        instant = ParameterUtil.parseDateHour(value.toString());
      } catch (DateTimeParseException ignored) {
        try {
          instant = ParameterUtil.parseDate(value.toString());
        } catch (Exception ignoredInner) {
        }
      }

      if (instant == null) {
        throw new ArgumentParserException(
            String.format("could not parse date/datehour for parameter '%s'", arg.textualName()),
            parser);
      }

      attrs.put(arg.getDest(), instant);
    }

    @Override
    public void onAttach(Argument arg) {
    }

    @Override
    public boolean consumeArgument() {
      return true;
    }
  }
}
