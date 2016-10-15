/*
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

import com.google.common.base.Throwables;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.core.Services;
import com.spotify.apollo.environment.ApolloEnvironment;
import com.spotify.apollo.environment.ApolloEnvironmentModule;
import com.spotify.apollo.http.client.HttpClientModule;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventSerializer;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;

import okio.ByteString;

public final class Main {

  private static final String ENV_VAR_PREFIX = "STYX_CLI";
  private static final String STYX_CLI_API = "http://styx.example.com/api/v1/cli";
  private static final int TTL_REQUEST = 30;

  private static final String COMPONENT_DEST = "component";
  private static final String WORKFLOW_DEST = "workflow";
  private static final String PARAMETER_DEST = "parameter";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
      .registerModule(new Jdk8Module());

  private Main() {
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final Service cliService = Services.usingName("styx-cli")
        .withEnvVarPrefix(ENV_VAR_PREFIX)
        .withModule(ApolloEnvironmentModule.create())
        .withModule(HttpClientModule.create())
        .build();

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("styx")
        .description("Styx CLI");

    final Subparsers subCommands = parser.addSubparsers()
        .title("commands")
        .metavar(" ");

    Command.ACTIVE_STATES.parser(subCommands);

    Subparser events = Command.EVENTS.parser(subCommands);
    addWorkflowInstanceArguments(events);

    Subparser trigger = Command.TRIGGER.parser(subCommands);
    addWorkflowInstanceArguments(trigger);

    Subparser retry = Command.RETRY.parser(subCommands);
    addWorkflowInstanceArguments(retry);

    final Argument plain = parser.addArgument("-p", "--plain")
        .help("plain output")
        .setDefault(false)
        .action(Arguments.storeTrue());

    final Argument verbose = parser.addArgument("-v", "--verbose")
        .help("increase verbosity")
        .action(Arguments.count());

    try (Service.Instance i = cliService.start()) {
      final Service.Signaller signaller = i.getSignaller();
      final Namespace namespace = parser.parseArgs(args);
      final int level = namespace.getInt(verbose.getDest());
      final boolean plainOutput = namespace.getBoolean(plain.getDest());
      final Command command = namespace.get(Command.DEST);
      final CliOutput cli = plainOutput
          ? new PlainCliOutput()
          : new PrettyCliOutput(level);

      cli.parsed(namespace);
      cli.header(command);

      final Client client = getClient(i);

      switch (command) {
        case ACTIVE_STATES:
          activeStates(client, cli, signaller);
          break;

        case EVENTS:
          eventsForWorkflowInstance(client, cli, signaller, namespace);
          break;

        case TRIGGER:
          triggerWorkflowInstance(client, cli, signaller, namespace);
          break;

        case RETRY:
          retryWorkflowInstance(client, cli, signaller, namespace);
          break;

        default:
          throw new RuntimeException("Unrecognized command: " + command);
      }

      i.waitForShutdown();
    } catch (HelpScreenException h) {
      System.exit(1);
    } catch (ArgumentParserException e) {
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      parser.printHelp(pw);
      new PrettyCliOutput(0).parseError(e, sw.toString());
      System.exit(2);
    }
  }

  private static void activeStates(Client client, CliOutput cliOutput, Service.Signaller signaller) {
    final Request request = Request.forUri(STYX_CLI_API + "/activeStates");
    client.send(request.withTtl(Duration.ofSeconds(TTL_REQUEST))).whenComplete((response, t) -> {
      byte[] bytes = response.payload().get().toByteArray();
      try {
        ActiveStatesPayload activeStatePayload = OBJECT_MAPPER.readValue(
            bytes, ActiveStatesPayload.class);
        cliOutput.printActiveStates(activeStatePayload);
      } catch (IOException e) {
        cliOutput.apiError(e);
      }
      signaller.signalShutdown();
    }).exceptionally(e -> {
      cliOutput.apiError(e);
      signaller.signalShutdown();
      return null;
    });
  }

  private static void eventsForWorkflowInstance(
      Client client,
      CliOutput cliOutput,
      Service.Signaller signaller,
      Namespace namespace) {

    String component = namespace.getString(COMPONENT_DEST);
    String workflow = namespace.getString(WORKFLOW_DEST);
    String parameter = namespace.getString(PARAMETER_DEST);
    String uri = String.format("%s/events/%s/%s/%s", STYX_CLI_API, component, workflow, parameter);
    final Request request = Request.forUri(uri);
    client.send(request).whenComplete((response, t) -> {

      byte[] bytes = response.payload().get().toByteArray();

      try {
        final EventsPayload eventsPayload = OBJECT_MAPPER.readValue(bytes, EventsPayload.class);
        cliOutput.printEvents(eventsPayload);
      } catch (IOException e) {
        cliOutput.apiError(e);
      }

      signaller.signalShutdown();
    }).exceptionally(e -> {
      cliOutput.apiError(e);
      signaller.signalShutdown();
      return null;
    });
  }

  private static void triggerWorkflowInstance(
      Client client,
      CliOutput cliOutput,
      Service.Signaller signaller,
      Namespace namespace) {

    String component = namespace.getString(COMPONENT_DEST);
    String workflow = namespace.getString(WORKFLOW_DEST);
    String parameter = namespace.getString(PARAMETER_DEST);
    WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(component, workflow), parameter);

    try {
      final ByteString payload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(workflowInstance));
      String uri = String.format("%s/trigger/", STYX_CLI_API);
      final Request request = Request.forUri(uri, "POST").withPayload(payload);
      client.send(request).whenComplete((response, t) -> {
        cliOutput.printResponse(response);
        signaller.signalShutdown();
      }).exceptionally(e -> {
        cliOutput.apiError(e);
        signaller.signalShutdown();
        return null;
      });
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  private static void retryWorkflowInstance(
      Client client,
      CliOutput cliOutput,
      Service.Signaller signaller,
      Namespace namespace) {

    String component = namespace.getString(COMPONENT_DEST);
    String workflow = namespace.getString(WORKFLOW_DEST);
    String parameter = namespace.getString(PARAMETER_DEST);
    WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(component, workflow), parameter);

    Event retry = Event.retry(workflowInstance);
    EventSerializer.PersistentEvent persistentEvent =
        EventSerializer.convertEventToPersistentEvent(retry);
    try {
      final ByteString payload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(persistentEvent));
      String uri = String.format("%s/events/", STYX_CLI_API);
      final Request request = Request.forUri(uri, "POST").withPayload(payload);
      client.send(request).whenComplete((response, t) -> {
        cliOutput.printResponse(response);
        signaller.signalShutdown();
      }).exceptionally(e -> {
        cliOutput.apiError(e);
        signaller.signalShutdown();
        return null;
      });
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Client getClient(Service.Instance i) {
    final ApolloEnvironment environment = ApolloEnvironmentModule.environment(i);
    return environment.environment().client();
  }

  private static void addWorkflowInstanceArguments(Subparser events) {
    events.addArgument(COMPONENT_DEST)
        .help("Component id");
    events.addArgument(WORKFLOW_DEST)
        .help("Workflow id (legacy Endpoint)");
    events.addArgument(PARAMETER_DEST)
        .help("Parameter identifying the workflow instance, e.g. '2016-09-14' or '2016-09-14T17'");
  }

  enum Command {
    ACTIVE_STATES("List active states", "ls"),
    EVENTS("events", "e"),
    TRIGGER("trigger", "t"),
    RETRY("retry", "r");

    static final String DEST = "command";

    private final String help;
    private final String[] aliases;

    Command(String help, String ... aliases) {
      this.help = help;
      this.aliases = aliases;
    }

    public Subparser parser(Subparsers subCommands) {
      return subCommands
          .addParser(name().toLowerCase())
          .aliases(aliases)
          .setDefault(DEST, this)
          .help(help);
    }
  }
}
