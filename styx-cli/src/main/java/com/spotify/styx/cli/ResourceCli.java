/*-
 * -\-\-
 * Spotify Styx CLI
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import static com.spotify.styx.serialization.Json.deserialize;
import static com.spotify.styx.serialization.Json.serialize;

import com.spotify.apollo.Request;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.model.Resource;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import okio.ByteString;

class ResourceCli {

  private static final String RESOURCE_ID = "id";
  private static final String RESOURCE_CONCURRENCY = "concurrency";

  static Subparsers registerCommands(Subparsers subparsers) {
    final Subparsers resourceParser = Main.Command.RESOURCE.parser(subparsers)
        .addSubparsers().title("commands").metavar(" ");

    final Subparser show = ResourceCommand.SHOW.parser(resourceParser);
    show.addArgument(RESOURCE_ID).help("Resource ID");

    final Subparser edit = ResourceCommand.EDIT.parser(resourceParser);
    edit.addArgument(RESOURCE_ID).help("Resource ID");
    edit.addArgument("--concurrency").dest(RESOURCE_CONCURRENCY)
        .help("set the concurrency value for the resource")
        .type(Integer.class);

    final Subparser list = ResourceCommand.LIST.parser(resourceParser);

    final Subparser create = ResourceCommand.CREATE.parser(resourceParser);
    create.addArgument(RESOURCE_ID).help("Resource ID");
    create.addArgument(RESOURCE_CONCURRENCY)
        .help("The concurrency of this resource")
        .type(Integer.class);

    return subparsers;
  }

  enum ResourceCommand {
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

    public Subparser parser(Subparsers subCommands) {
      final Subparser subparser = subCommands
          .addParser(name().toLowerCase())
          .setDefault(Main.SUBCOMMAND_DEST, this)
          .description(description)
          .help(description);

      if (alias != null && !alias.isEmpty()) {
        subparser.aliases(alias);
      }

      return subparser;
    }
  }

  static void runCommand(Main main, ResourceCommand resourceCommand)
      throws InterruptedException, ExecutionException, IOException, ArgumentParserException {
    switch (resourceCommand) {
      case CREATE:
        create(main);
        break;
      case EDIT:
        edit(main);
        break;
      case SHOW:
        show(main);
        break;
      case LIST:
        list(main);
        break;
      default:
        // parsing unknown command will fail so this would only catch non-exhaustive switches
        throw new ArgumentParserException(
            String.format("Unrecognized resource command: %s", resourceCommand),
            main.parser.parser);
    }
  }

  private static void create(Main main) throws ExecutionException, InterruptedException, IOException {
    final String id = main.namespace.getString(RESOURCE_ID);
    final int concurrency = main.namespace.getInt(RESOURCE_CONCURRENCY);

    final ByteString payload = serialize(Resource.create(id, concurrency));
    final ByteString response = main.send(
        Request.forUri(main.apiUrl("resources"), "POST").withPayload(payload));
    final Resource resource = deserialize(response, Resource.class);
    main.cliOutput.printResources(Collections.singletonList(resource));
  }

  private static void edit(Main main) throws ExecutionException, InterruptedException, IOException {
    final String id = main.namespace.getString(RESOURCE_ID);
    final Integer concurrency = main.namespace.getInt(RESOURCE_CONCURRENCY);

    final ByteString getResponse = main.send(Request.forUri(main.apiUrl("resources", id)));
    Resource resource = deserialize(getResponse, Resource.class);
    if (concurrency != null) {
      resource = Resource.create(resource.id(), concurrency);
    }
    final ByteString putPayload = serialize(resource);
    final ByteString putResponse = main.send(
        Request.forUri(main.apiUrl("resources", id), "PUT").withPayload(putPayload));

    final Resource newResource = deserialize(putResponse, Resource.class);
    main.cliOutput.printResources(Collections.singletonList(newResource));
  }

  private static void show(Main main) throws ExecutionException, InterruptedException, IOException {
    final String id = main.namespace.getString(RESOURCE_ID);
    final ByteString response = main.send(Request.forUri(main.apiUrl("resources", id)));
    final Resource resource = deserialize(response, Resource.class);
    main.cliOutput.printResources(Collections.singletonList(resource));
  }

  private static void list(Main main)
      throws IOException, ExecutionException, InterruptedException {
    final ByteString response = main.send(Request.forUri(main.apiUrl("resources")));
    final ResourcesPayload resources = deserialize(response, ResourcesPayload.class);
    main.cliOutput.printResources(resources.resources());
  }
}
