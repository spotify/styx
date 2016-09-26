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

import com.spotify.apollo.Response;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import okio.ByteString;

/**
 * Cli printing interface
 */
public interface CliOutput {

  void parsed(Namespace namespace);

  void parseError(ArgumentParserException e, String help);

  void apiError(Throwable throwable);

  void header(Main.Command command);

  void printActiveStates(ActiveStatesPayload activeStatesPayload);

  void printEvents(EventsPayload eventsPayload);

  void printResponse(Response<ByteString> response);
}
