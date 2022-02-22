/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.spotify.apollo.Response;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.util.Optional;
import javaslang.control.Try;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkflowActionAuthorizerTest {

  private static final Workflow WORKFLOW = Workflow.create(WORKFLOW_ID.componentId(),
      TestData.FULL_WORKFLOW_CONFIGURATION);

  private static final Workflow WORKFLOW_WITHOUT_SA = Workflow.create(WORKFLOW_ID.componentId(),
      TestData.HOURLY_WORKFLOW_CONFIGURATION);

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private Storage storage;
  @Mock private ServiceAccountUsageAuthorizer authorizer;
  @Mock private ActionAuthorizer actionAuthorizer;
  @Mock private Middlewares.AuthContext ac;
  @Mock private GoogleIdToken idToken;

  private WorkflowActionAuthorizer sut;
  private boolean isFlyteWorkflow = false;

  class ThrowActionAuthorizer implements ActionAuthorizer {
    @Override
    public void authorizeCreateOrUpdateWorkflowAction(final Workflow workflow) {
      throw  new ResponseException(Response.forStatus(FORBIDDEN));
    }

    @Override
    public void authorizeDeleteWorkflowAction(final Workflow workflow) {
      throw  new ResponseException(Response.forStatus(FORBIDDEN));
    }

    @Override
    public void authorizePatchStateWorkflowAction(final Workflow workflow) {
      throw  new ResponseException(Response.forStatus(FORBIDDEN));
    }
  }

  @Before
  public void setUp() throws Exception {
    sut = new WorkflowActionAuthorizer(storage, authorizer, actionAuthorizer);
  }

  @Test
  public void authorizeWorkflowActionWithIdShouldFailIfStorageReadFails() throws IOException {
    final IOException cause = new IOException();
    when(storage.workflow(any())).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizeWorkflowAction(ac, WORKFLOW.id());
  }

  @Test
  public void authorizeWorkflowActionWithIdShouldFailIfWorkflowNotFound() throws IOException {
    when(storage.workflow(any())).thenReturn(Optional.empty());
    exception.expect(ResponseException.class);
    sut.authorizeWorkflowAction(ac, WORKFLOW.id());
  }

  @Test
  public void authorizeWorkflowActionWithIdShouldPass() throws IOException {
    when(storage.workflow(any())).thenReturn(Optional.of(WORKFLOW));
    when(ac.user()).thenReturn(Optional.of(idToken));
    assertThat(Try.run(() -> sut.authorizeWorkflowAction(ac, WORKFLOW.id())).isSuccess(), is(true));
    verify(authorizer).authorizeServiceAccountUsage(
        WORKFLOW.id(), isFlyteWorkflow, WORKFLOW.configuration().serviceAccount().orElseThrow(), idToken);
  }

  @Test
  public void authorizeWorkflowActionShouldFailIfNoUser() {
    when(ac.user()).thenReturn(Optional.empty());
    exception.expect(AssertionError.class);
    sut.authorizeWorkflowAction(ac, WORKFLOW);
  }

  @Test
  public void authorizeWorkflowActionShouldPassIfNoServiceAccountConfigured() {
    when(ac.user()).thenReturn(Optional.of(idToken));
    assertThat(Try.run(() -> sut.authorizeWorkflowAction(ac, WORKFLOW_WITHOUT_SA)).isSuccess(), is(true));
  }

  @Test
  public void authorizeWorkflowActionShouldPassIfAuthorizerPasses() {
    when(ac.user()).thenReturn(Optional.of(idToken));
    assertThat(Try.run(() -> sut.authorizeWorkflowAction(ac, WORKFLOW)).isSuccess(), is(true));
    verify(authorizer).authorizeServiceAccountUsage(
        WORKFLOW.id(), isFlyteWorkflow, WORKFLOW.configuration().serviceAccount().orElseThrow(), idToken);
  }

  @Test
  public void authorizeWorkflowActionShouldFailIfAuthorizerFails() {
    when(ac.user()).thenReturn(Optional.of(idToken));
    final ResponseException cause = new ResponseException(Response.forStatus(FORBIDDEN));
    doThrow(cause).when(authorizer).authorizeServiceAccountUsage(any(), anyBoolean(), any(), any());
    exception.expect(is(cause));
    sut.authorizeWorkflowAction(ac, WORKFLOW);
    final Try<Void> invocation = Try.run(() -> sut.authorizeWorkflowAction(ac, WORKFLOW));
    assertThat(invocation.isFailure(), is(true));
    verify(authorizer).authorizeServiceAccountUsage(
        WORKFLOW.id(), isFlyteWorkflow, WORKFLOW.configuration().serviceAccount().orElseThrow(), idToken);
    assertThat(invocation.getCause(), is(cause));
  }

  @Test
  public void authorizePatchStateWorkflowActionWithIdShouldFailIfWorkflowNotFound() throws IOException {
    when(storage.workflow(any())).thenReturn(Optional.empty());
    exception.expect(ResponseException.class);
    sut.authorizePatchStateWorkflowAction(WORKFLOW.id());
  }

  @Test
  public void authorizePatchStateWorkflowActionWithIdShouldFailIfStorageReadFails() throws IOException {
    final IOException cause = new IOException();
    when(storage.workflow(any())).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizePatchStateWorkflowAction(WORKFLOW.id());
  }

  @Test
  public void authorizePatchStateWorkflowActionFailIfActionAutorizerComplains() {
    sut = new WorkflowActionAuthorizer(storage, authorizer, new ThrowActionAuthorizer());
    exception.expect(ResponseException.class);
    sut.authorizePatchStateWorkflowAction(WORKFLOW.id());
  }

  @Test
  public void authorizeCreateUpdateWorkflowActionFailIfActionAutorizerComplains() {
    sut = new WorkflowActionAuthorizer(storage, authorizer, new ThrowActionAuthorizer());
    exception.expect(ResponseException.class);
    sut.authorizeCreateOrUpdateWorkflowAction(WORKFLOW);
  }

  @Test
  public void authorizeDeleteWorkflowActionFailIfActionAutorizerComplains() {
    sut = new WorkflowActionAuthorizer(storage, authorizer, new ThrowActionAuthorizer());
    exception.expect(ResponseException.class);
    sut.authorizeDeleteWorkflowAction(WORKFLOW);
  }

}
