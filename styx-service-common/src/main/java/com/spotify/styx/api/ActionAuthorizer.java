/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2021 Spotify AB
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


package com.spotify.styx.api;

import com.spotify.apollo.Environment;
import com.spotify.styx.model.Workflow;

/**
 * Action Authorizer can be used to implement an extra authorization system for destructive action perform on workflow though POST, DELETE AND PATCH api
 * commands.
 */
public interface ActionAuthorizer {

  interface Factory{
    ActionAuthorizer create(Environment environment);

    Factory DEFAULT = e -> Nop.INSTANCE;
  }

  /**
   * Authorize a workflow to be created or updated
   *
   * @throws ResponseException if not authorized.
   */
  void authorizeCreateOrUpdateWorkflowAction(final Workflow workflow);

  /**
   * Authorize a workflow to be deleted
   *
   * @throws ResponseException if not authorized.
   */
  void authorizeDeleteWorkflowAction(final Workflow workflow);

  /**
   * Authorize a workflow to be patched
   *
   * @throws ResponseException if not authorized.
   */
  void authorizePatchStateWorkflowAction(final Workflow workflow);

  class Nop implements ActionAuthorizer {

    static final Nop INSTANCE = new Nop();

    @Override
    public void authorizeCreateOrUpdateWorkflowAction(final Workflow workflow) {
      // nop
    }

    @Override
    public void authorizeDeleteWorkflowAction(final Workflow workflow) {
      // nop
    }

    @Override
    public void authorizePatchStateWorkflowAction(final Workflow workflow) {
      // nop
    }
  }
}
