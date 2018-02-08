/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.state;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link OutputHandler} that fans out to a list of other outputHandlers.
 */
class FanOutputHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FanOutputHandler.class);

  private final Iterable<OutputHandler> outputHandlers;

  FanOutputHandler(Iterable<OutputHandler> outputHandlers) {
    this.outputHandlers = Objects.requireNonNull(outputHandlers);
  }

  @Override
  public void transitionInto(RunState state) {
    for (OutputHandler handler : outputHandlers) {
      try {
        handler.transitionInto(state);
      } catch (Throwable e) {
        LOG.warn("Output handler {} threw", handler, e);
        throw e;
      }
    }
  }
}
