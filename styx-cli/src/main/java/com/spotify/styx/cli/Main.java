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
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.ParameterUtil;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.format.DateTimeParseException;
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
  private static final String STYX_API_ENDPOINT = "/api/v2";
  private static final int TTL_REQUEST = 90;

  private static final String COMMAND_DEST = "command";
  static final String SUBCOMMAND_DEST = "subcommand";
  private static final String COMPONENT_DEST = "component";
  private static final String WORKFLOW_DEST = "workflow";
  private static final String PARAMETER_DEST = "parameter";

  private static final int EXIT_CODE_SUCCESS = 0;
  private static final int EXIT_CODE_UNKNOWN_ERROR = 1;
  private static final int EXIT_CODE_ARGUMENT_ERROR = 2;
  private static final int EXIT_CODE_API_ERROR = 3;
  private static final String STYX_CLI_VERSION =
      "Styx CLI " + Main.class.getPackage().getImplementationVersion();

  final StyxCliParser parser;
  final Namespace namespace;
  final CliOutput cliOutput;

  private final String apiHost;
  private final Service cliService;
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
    final boolean jsonOutput = namespace.getBoolean(parser.json.getDest());
    final CliOutput cliOutput;
    if (jsonOutput) {
      cliOutput = new JsonCliOutput();
    } else if (plainOutput) {
      cliOutput = new PlainCliOutput();
    } else {
      cliOutput = new PrettyCliOutput();
    }

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
          BackfillCli.runCommand(this, namespace.get(SUBCOMMAND_DEST));
          break;

        case RESOURCE:
          ResourceCli.runCommand(this, namespace.get(SUBCOMMAND_DEST));
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

  private void activeStates() throws IOException, ExecutionException, InterruptedException {
    String uri = apiUrl("status", "activeStates");
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
    final String workflow = workflowInstance.workflowId().id();
    final String parameter = workflowInstance.parameter();

    final ByteString response = send(
        Request.forUri(apiUrl("status", "events", component, workflow, parameter))
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
    final ByteString payload = serialize(getWorkflowInstance(namespace));
    send(Request.forUri(apiUrl("scheduler", "trigger"), "POST").withPayload(payload));
  }

  private void haltWorkflowInstance()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    final ByteString payload = serialize(Event.halt(getWorkflowInstance(namespace)));
    send(Request.forUri(apiUrl("scheduler", "events"), "POST").withPayload(payload));
  }

  private void retryWorkflowInstance()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    final ByteString payload = serialize(Event.dequeue(getWorkflowInstance(namespace)));
    send(Request.forUri(apiUrl("scheduler", "events"), "POST").withPayload(payload));
  }

  private static WorkflowInstance getWorkflowInstance(Namespace namespace) {
    return WorkflowInstance.create(
        WorkflowId.create(
            namespace.getString(COMPONENT_DEST),
            namespace.getString(WORKFLOW_DEST)),
        namespace.getString(PARAMETER_DEST));
  }

  String apiUrl(CharSequence... parts) {
    return "http://" + apiHost + STYX_API_ENDPOINT + "/" + String.join("/", parts);
  }

  ByteString send(Request request) throws ExecutionException, InterruptedException {
    final Response<ByteString> response =
        client.send(request.withHeader("User-Agent", STYX_CLI_VERSION)).toCompletableFuture()
            .get();

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

  static class StyxCliParser {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("styx")
        .description("Styx CLI")
        .version(STYX_CLI_VERSION);

    final Subparsers subCommands = parser.addSubparsers().title("commands").metavar(" ");

    final Subparsers backfillCommands = BackfillCli.registerCommands(subCommands);
    final Subparsers resourceCommands = ResourceCli.registerCommands(subCommands);

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

    final Argument json = parser.addArgument("--json")
        .help("json output")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Argument plain = parser.addArgument("-p", "--plain")
        .help("plain output")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Argument version = parser.addArgument("--version").action(Arguments.version());

    private static Subparser addWorkflowInstanceArguments(Subparser subparser) {
      subparser.addArgument(COMPONENT_DEST)
          .help("Component id");
      subparser.addArgument(WORKFLOW_DEST)
          .help("Workflow id");
      subparser.addArgument(PARAMETER_DEST)
          .help("Parameter identifying the workflow instance, e.g. '2016-09-14' or '2016-09-14T17'");
      return subparser;
    }
  }

  enum Command {
    LIST("ls", "List active workflow instances"),
    EVENTS("e", "List events for a workflow instance"),
    HALT("h", "Halt a workflow instance"),
    TRIGGER("t", "Trigger a completed workflow instance"),
    RETRY("r", "Retry a workflow instance that is in a waiting state"),
    RESOURCE(null, "Commands related to resources"),
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

  static class PartitionAction implements ArgumentAction {

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
    }

    @Override
    public boolean consumeArgument() {
      return true;
    }
  }
}
