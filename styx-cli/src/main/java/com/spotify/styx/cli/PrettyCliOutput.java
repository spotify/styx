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

import static com.spotify.apollo.Status.OK;
import static com.spotify.styx.cli.CliUtil.colored;
import static com.spotify.styx.cli.CliUtil.formatTimestamp;
import static com.spotify.styx.model.EventSerializer.PersistentEvent;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.MAGENTA;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.spotify.apollo.Response;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.ActiveStatesPayload.ActiveState;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.api.cli.EventsPayload.TimestampedPersistentEvent;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.EventUtil;
import java.util.SortedMap;
import java.util.SortedSet;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import okio.ByteString;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PrettyCliOutput implements CliOutput {

  private static final Logger LOG = LoggerFactory.getLogger(PrettyCliOutput.class);

  PrettyCliOutput(int level) {
    init(level);
  }

  @Override
  public void parsed(Namespace namespace) {
    LOG.debug("Parsed namespace {}", namespace);
  }

  @Override
  public void parseError(ArgumentParserException e, String help) {
    LOG.warn(e.getMessage());
    LOG.info(help);
  }

  @Override
  public void apiError(Throwable throwable) {
    LOG.warn("An API error occurred", throwable);
  }

  @Override
  public void header(Main.Command command) {
    LOG.info("{}", colored(MAGENTA, "      _                   "));
    LOG.info("{}", colored(MAGENTA, "   __| |_ _  ___ __       "));
    LOG.info("{}", colored(MAGENTA, "  (_-<  _| || \\ \\ /     "));
    LOG.info("{}", colored(MAGENTA, "  /__/\\__|\\_, /_\\_\\   "));
    LOG.info("{}", colored(MAGENTA, "          |__/            "));
    LOG.info("");
    LOG.info("> {}", colored(CYAN, command));
    LOG.info("");
  }

  @Override
  public void printActiveStates(ActiveStatesPayload activeStatesPayload) {
    SortedMap<WorkflowId, SortedSet<ActiveState>> groupedActiveStates =
        CliUtil.groupActiveStates(activeStatesPayload.activeStates());

    LOG.info(String.format("%-30.30s %-30.30s %-30.30s %-30.30s",
                             "WORKFLOW INSTANCE",
                             "STATE",
                             "LAST EXECUTION ID",
                             "PREVIOUS EXECUTION INFO"));
    LOG.info(String.format("%-30.30s %-30.30s %-30.30s %-30.30s",
                           "-----------------------",
                           "-----------------------",
                           "-----------------------",
                           "-----------------------"));
    for (WorkflowId workflowId : groupedActiveStates.keySet()) {
      LOG.info("{} <> {}",
               colored(CYAN, workflowId.componentId()),
               colored(CYAN, workflowId.endpointId()));
      for (ActiveState activeState : groupedActiveStates.get(workflowId)) {
        final Ansi previousExecutionInfo;
        if (activeState.previousExecutionLastEvent().isPresent()) {
          final PersistentEvent persistentEvent = activeState.previousExecutionLastEvent().get();
          final String message = CliUtil.lastExecutionMessage(persistentEvent.toEvent());
          final Ansi.Color messageColor = persistentEvent.toEvent().accept(LastExecutionColor.LAST_EXECUTION_COLOR);
          previousExecutionInfo = colored(messageColor, message);
        } else {
          previousExecutionInfo = colored(WHITE, "No data found");
        }
        LOG.info(String.format("%s %-27.30s %-30.30s %-30.30s %s",
                               colored(CYAN, "\\_"),
                               activeState.workflowInstance().parameter(),
                               activeState.state(),
                               activeState.lastExecutionId(),
                               previousExecutionInfo));
      }
    }
  }

  @Override
  public void printEvents(EventsPayload eventsPayload) {
    LOG.info(String.format("%-25.25s %-25.25s %-25.25s",
                           "TIME",
                           "EVENT",
                           "DATA"));
    LOG.info(String.format("%-25.25s %-25.25s %-25.25s",
                           "-------------------",
                           "-------------------",
                           "-------------------"));
    for (TimestampedPersistentEvent timestampedEvent : eventsPayload.events()) {
      LOG.info(String.format("%-25.25s %-25.25s %s",
                             formatTimestamp(timestampedEvent.timestamp()),
                             EventUtil.name(timestampedEvent.event().toEvent()),
                             CliUtil.data(timestampedEvent.event().toEvent())));
    }
  }

  @Override
  public void printResponse(Response<ByteString> response) {
    if (OK.equals(response.status())) {
      LOG.info(String.valueOf(colored(GREEN, "Success")));
    } else {
      LOG.info(String.valueOf(colored(MAGENTA, "Code: " + response.status().code())));
      LOG.info(String.valueOf(colored(MAGENTA, response.status().reasonPhrase())));
    }
  }

  private void init(int level) {
    final ch.qos.logback.classic.Logger
        logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("com.spotify.styx.cli");

    if (level > 1) {
      logger.setLevel(Level.TRACE);
    } else if (level > 0) {
      logger.setLevel(Level.DEBUG);
    }

    if (level > 0) {
      final LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
      final ch.qos.logback.classic.Logger
          rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
          ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
      final ConsoleAppender<ILoggingEvent> stdout =
          (ConsoleAppender<ILoggingEvent>) rootLogger.getAppender("STDOUT");
      final PatternLayoutEncoder layout = new PatternLayoutEncoder();
      layout.setPattern("%gray(%d{HH:mm:ss.SSS}) %highlight(| %-5level| %-10logger{0}) "
                        + "%green(|>) %msg%n");
      layout.setContext(lc);
      layout.start();

      stdout.setEncoder(layout);
      stdout.start();
    }
  }

  private enum LastExecutionColor implements EventVisitor<Ansi.Color> {
    LAST_EXECUTION_COLOR;

    @Override
    public Ansi.Color terminate(WorkflowInstance workflowInstance, int exitCode) {
      switch (exitCode) {
        case CliUtil.SUCCESS_EXIT_CODE:
          return GREEN;
        case CliUtil.MISSING_DEPENDENCIES_EXIT_CODE:
          return YELLOW;
        default:
          return MAGENTA;
      }
    }

    @Override
    public Ansi.Color runError(WorkflowInstance workflowInstance, String message) {
      return MAGENTA;
    }

    @Override
    public Ansi.Color timeTrigger(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return WHITE;
    }

    @Override
    public Ansi.Color created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return WHITE;
    }

    @Override
    public Ansi.Color started(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color success(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return WHITE;
    }

    @Override
    public Ansi.Color retry(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color stop(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color timeout(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color halt(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return WHITE;
    }

    @Override
    public Ansi.Color submitted(WorkflowInstance workflowInstance, String executionId) {
      return WHITE;
    }
  }
}
