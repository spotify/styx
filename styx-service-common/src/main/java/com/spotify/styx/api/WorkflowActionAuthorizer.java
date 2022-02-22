/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.Middlewares.AuthContext;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class WorkflowActionAuthorizer {

  private final Storage storage;
  private final ServiceAccountUsageAuthorizer serviceAccountUsageAuthorizer;
  private final ActionAuthorizer actionAuthorizer;

  public WorkflowActionAuthorizer(Storage storage,
                                  ServiceAccountUsageAuthorizer serviceAccountUsageAuthorizer,
                                  ActionAuthorizer actionAuthorizer) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.serviceAccountUsageAuthorizer = Objects.requireNonNull(serviceAccountUsageAuthorizer,
        "serviceAccountUsageAuthorizer");
    this.actionAuthorizer = Objects.requireNonNull(actionAuthorizer);
  }

  public void authorizeWorkflowAction(AuthContext ac, WorkflowId workflowId) {
    final Optional<Workflow> workflowOpt;
    try {
      workflowOpt = storage.workflow(workflowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Workflow workflow = workflowOpt.orElseThrow(() -> new ResponseException(
        Response.forStatus(Status.NOT_FOUND.withReasonPhrase("workflow not found"))));
    authorizeWorkflowAction(ac, workflow);
  }

  public void authorizeWorkflowAction(AuthContext ac, Workflow workflow) {
    final GoogleIdToken idToken = ac.user().orElseThrow(AssertionError::new);
    final Optional<String> serviceAccount = workflow.configuration().serviceAccount();
    if (serviceAccount.isEmpty()) {
      return;
    }
    serviceAccountUsageAuthorizer.authorizeServiceAccountUsage(workflow.id(), serviceAccount.get(), idToken);
  }

  public void authorizeCreateOrUpdateWorkflowAction(Workflow workflow){
    actionAuthorizer.authorizeCreateOrUpdateWorkflowAction(workflow);
  }
  public void authorizeDeleteWorkflowAction(Workflow workflow) {
    actionAuthorizer.authorizeDeleteWorkflowAction(workflow);
  }
  public void authorizePatchStateWorkflowAction(WorkflowId workflowId) {
    final Optional<Workflow> workflowOpt;
    try {
      workflowOpt = storage.workflow(workflowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final Workflow workflow = workflowOpt.orElseThrow(() -> new ResponseException(
        Response.forStatus(Status.NOT_FOUND.withReasonPhrase("workflow not found"))));
    actionAuthorizer.authorizePatchStateWorkflowAction(workflow);
  }
}
