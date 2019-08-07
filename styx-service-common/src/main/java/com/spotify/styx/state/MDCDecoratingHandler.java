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

import com.google.common.io.Closer;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.MDC;

/**
 * A {@link OutputHandler} that decorates the logging {@link MDC} with workflow instance state information.
 */
class MDCDecoratingHandler implements OutputHandler {

  private static final String WFI_ID = "wfi-id";
  private static final String WFI_STATE_COUNTER = "wfi-state-counter";
  private static final String WFI_STATE_NAME = "wfi-state-name";

  private final OutputHandler delegate;

  MDCDecoratingHandler(OutputHandler delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public void transitionInto(RunState runState, EventRouter eventRouter) {
    try (var closer = Closer.create()) {
      closer.register(MDC.putCloseable(WFI_ID, runState.workflowInstance().toString()));
      closer.register(MDC.putCloseable(WFI_STATE_COUNTER, Long.toString(runState.counter())));
      closer.register(MDC.putCloseable(WFI_STATE_NAME, runState.state().toString()));
      delegate.transitionInto(runState, eventRouter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
