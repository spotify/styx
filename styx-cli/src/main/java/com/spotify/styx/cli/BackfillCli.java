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
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.BackfillInput;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import okio.ByteString;

class BackfillCli {

  private static final String UTF_8 = "UTF-8";
  private static final String WORKFLOW = "workflow";
  private static final String COMPONENT = "component";
  private static final String SHOW_ALL = "showAll";
  private static final String CONCURRENCY = "concurrency";
  private static final String ID = "id";
  private static final String START = "start";
  private static final String END = "end";

  static Subparsers registerCommands(Subparsers subparsers) {
    final Subparsers backfillParser = Main.Command.BACKFILL.parser(subparsers)
            .addSubparsers().title("commands").metavar(" ");

    final Subparser show = BackfillCommand.SHOW.parser(backfillParser);
    show.addArgument("backfill").dest(ID).help("Backfill ID");

    final Subparser edit = BackfillCommand.EDIT.parser(backfillParser);
    edit.addArgument("backfill").help("Backfill ID");
    edit.addArgument("--concurrency").dest(CONCURRENCY)
        .help("set the concurrency value for the backfill")
        .type(Integer.class);

    final Subparser halt = BackfillCommand.HALT.parser(backfillParser);
    halt.addArgument("backfill").help("Backfill ID");

    final Subparser list = BackfillCommand.LIST.parser(backfillParser);
    list.addArgument("-w", "--workflow").dest(WORKFLOW)
        .help("only show backfills for WORKFLOW");
    list.addArgument("-c", "--component").dest(COMPONENT)
        .help("only show backfills for COMPONENT");
    list.addArgument("-a", "--show-all").dest(SHOW_ALL)
        .setDefault(false)
        .action(Arguments.storeTrue())
        .help("show all backfills, even halted and all-triggered ones");

    final Main.PartitionAction partitionAction = new Main.PartitionAction();
    final Subparser create = BackfillCommand.CREATE.parser(backfillParser);
    create.addArgument(COMPONENT)
        .help("Component ID");
    create.addArgument(WORKFLOW)
        .help("Workflow ID");
    create.addArgument(START)
        .help("Start date/datehour (inclusive)")
        .action(partitionAction);
    create.addArgument(END)
        .help("End date/datehour (exclusive)")
        .action(partitionAction);
    create.addArgument(CONCURRENCY)
        .help("The number of jobs to run in parallel")
        .type(Integer.class);

    return backfillParser;
  }

  enum BackfillCommand {
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

  static void runCommand(Main main, BackfillCommand backfillCommand)
      throws IOException, InterruptedException, ArgumentParserException, ExecutionException {
    switch (backfillCommand) {
      case CREATE:
        create(main);
        break;
      case EDIT:
        edit(main);
        break;
      case HALT:
        halt(main);
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
            String.format("Unrecognized command: %s", backfillCommand),
            main.parser.parser);
    }

  }

  private static void create(Main main) throws ExecutionException, InterruptedException, IOException {
    final String component = main.namespace.getString(COMPONENT);
    final String workflow = main.namespace.getString(WORKFLOW);
    final String start = main.namespace.getString(START);
    final String end = main.namespace.getString(END);
    final int concurrency = main.namespace.getInt(CONCURRENCY);

    final BackfillInput backfillInput = BackfillInput.create(
        Instant.parse(start), Instant.parse(end), component, workflow, concurrency);

    final ByteString payload = serialize(backfillInput);
    final ByteString response = main.send(
        Request.forUri(main.apiUrl("backfills"), "POST").withPayload(payload));
    final Backfill backfill = deserialize(response, Backfill.class);
    main.cliOutput.printBackfill(backfill);
  }

  private static void edit(Main main) throws ExecutionException, InterruptedException, IOException {
    final Integer concurrency = main.namespace.getInt(CONCURRENCY);
    final String id = main.namespace.getString(ID);

    final ByteString getResponse = main.send(Request.forUri(main.apiUrl("backfills", id)));
    final BackfillPayload backfillPayload = deserialize(getResponse, BackfillPayload.class);
    final BackfillBuilder editedBackfillBuilder = backfillPayload.backfill().builder();
    if (concurrency != null) {
      editedBackfillBuilder.concurrency(concurrency);
    }
    final ByteString putPayload = serialize(editedBackfillBuilder.build());
    final ByteString putResponse = main.send(
        Request.forUri(main.apiUrl("backfills", id), "PUT").withPayload(putPayload));

    final Backfill newBackfill = deserialize(putResponse, Backfill.class);
    main.cliOutput.printBackfill(newBackfill);
  }

  private static void halt(Main main) throws ExecutionException, InterruptedException {
    final String id = main.namespace.getString(ID);
    main.send(Request.forUri(main.apiUrl("backfills", id), "DELETE"));
  }

  private static void show(Main main) throws ExecutionException, InterruptedException, IOException {
    final String id = main.namespace.getString(ID);
    final ByteString response = main.send(Request.forUri(main.apiUrl("backfills", id)));
    final BackfillPayload backfillStatus = deserialize(response, BackfillPayload.class);
    main.cliOutput.printBackfillPayload(backfillStatus);
  }

  private static void list(Main main)
      throws IOException, ExecutionException, InterruptedException {
    String uri = main.apiUrl("backfills");
    final List<String> queries = new ArrayList<>();
    if (main.namespace.getBoolean(SHOW_ALL)) {
      queries.add("showAll=true");
    }
    final String component = main.namespace.getString(COMPONENT);
    if (component != null) {
      queries.add("component=" + URLEncoder.encode(component, UTF_8));
    }
    final String workflow = main.namespace.getString(WORKFLOW);
    if (workflow != null) {
      queries.add("workflow=" + URLEncoder.encode(workflow, UTF_8));
    }
    if (!queries.isEmpty()) {
      uri += "?" + String.join("&", queries);
    }
    final ByteString response = main.send(Request.forUri(uri));
    final BackfillsPayload backfills = deserialize(response, BackfillsPayload.class);
    main.cliOutput.printBackfills(backfills.backfills());
  }
}
