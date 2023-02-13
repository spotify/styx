/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.api;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.styx.api.SchedulerResource.BASE;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.serialize;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.RESOURCE_IDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.TriggerListener;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.TriggerRequest;
import com.spotify.styx.model.TriggerResponse;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * API endpoints for interacting directly with the scheduler
 */
public class SchedulerResourceTest {

  private static final WorkflowInstance WFI = WorkflowInstance
      .create(TestData.WORKFLOW_ID, "12345");

  private final Workflow HOURLY_WORKFLOW = Workflow.create("styx",
                                                           TestData.HOURLY_WORKFLOW_CONFIGURATION);
  private final Workflow DAILY_WORKFLOW = Workflow.create("styx",
                                                          TestData.DAILY_WORKFLOW_CONFIGURATION);
  private final Workflow FULL_DAILY_WORKFLOW = Workflow.create("styx",
                                                               FULL_WORKFLOW_CONFIGURATION);
  private final Workflow WEEKLY_WORKFLOW = Workflow.create("styx",
                                                           TestData.WEEKLY_WORKFLOW_CONFIGURATION);
  private final Workflow MONTHLY_WORKFLOW = Workflow.create("styx",
                                                            TestData.MONTHLY_WORKFLOW_CONFIGURATION);

  private final Workflow FLYTE_WORKFLOW = Workflow.create("flytewf",
      TestData.FLYTE_WORKFLOW_CONFIGURATION);

  private Optional<Workflow> triggeredWorkflow = Optional.empty();
  private Optional<Instant> triggeredInstant = Optional.empty();

  @Mock private WorkflowValidator workflowValidator;

  private final Storage storage = mock(Storage.class);

  @Captor ArgumentCaptor<Trigger> triggerCaptor;

  @Mock private StateManager stateManager;
  @Mock private TriggerListener triggerListener;
  @Mock private WorkflowActionAuthorizer workflowActionAuthorizer;
  @Mock private GoogleIdToken idToken;
  @Mock private RequestAuthenticator requestAuthenticator;

  @Rule
  public ServiceHelper serviceHelper;

  public SchedulerResourceTest() {
    MockitoAnnotations.initMocks(this);
    serviceHelper = getServiceHelper(stateManager, storage);
  }

  @Before
  public void setUp() throws Exception {
    when(storage.workflow(WFI.workflowId())).thenReturn(Optional.of(FULL_DAILY_WORKFLOW));
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    when(requestAuthenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));
  }

  private ServiceHelper getServiceHelper(StateManager stateManager, Storage storage) {
    return ServiceHelper.create((environment) -> {
      final SchedulerResource schedulerResource = new SchedulerResource(
          stateManager,
          triggerListener,
          storage,
          () -> Instant.parse("2015-12-31T23:59:10.000Z"),
          workflowValidator, workflowActionAuthorizer);

      environment.routingEngine()
          .registerRoutes(schedulerResource.routes(requestAuthenticator).map(r ->
              r.withMiddleware(Middlewares.exceptionAndRequestIdHandler())));
    }, "styx").startTimeoutSeconds(30);
  }

  private Response<ByteString> requestAndWaitTriggerWorkflowInstance(TriggerRequest request)
      throws Exception {

    ByteString eventPayload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(request));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/trigger", eventPayload);

    return post.toCompletableFuture().get(1, MINUTES);
  }

  @Test
  public void retryShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    final Response<ByteString> response = serviceHelper.request("POST", BASE + "/retry", serialize(WFI))
        .toCompletableFuture().get(1, MINUTES);

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verifyZeroInteractions(stateManager);
  }

  @Test
  public void testRetry() throws Exception {
    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/retry", wfiPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager)
        .receive(Event.retryAfter(WFI, 0L));
  }

  @Test
  public void testRetryWithDelayParameter() throws Exception {
    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/retry?delay=500", wfiPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager).receive(Event.retryAfter(WFI, 500L));
  }

  @Test
  public void testRetryWithWrongDelayParameter() throws Exception {
    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/retry?delay=abc", wfiPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST
        .withReasonPhrase("Delay parameter could not be parsed"))));
    verify(stateManager, never()).receive(any());
  }

  @Test
  public void haltShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    final Response<ByteString> response = serviceHelper.request("POST", BASE + "/halt", serialize(WFI))
        .toCompletableFuture().get(1, MINUTES);

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verifyZeroInteractions(stateManager);
  }

  @Test
  public void testHalt() throws Exception {
    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/halt", wfiPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager)
        .receive(Event.halt(WFI));
  }

  @Test
  public void testInjectDequeueEvent() throws Exception {
    Event injectedEvent = Event.dequeue(WFI, RESOURCE_IDS);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/events", eventPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager).receive(Event.retryAfter(WFI, 0L));
  }

  @Test
  public void testInjectEventShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    final Response<ByteString> response =
        serviceHelper.request("POST", BASE + "/events", serialize(Event.dequeue(WFI, RESOURCE_IDS)))
        .toCompletableFuture().get(1, MINUTES);

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verifyZeroInteractions(stateManager);
  }

  @Test
  public void testInjectHaltEvent() throws Exception {
    Event injectedEvent = Event.halt(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/events", eventPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager).receive(Event.halt(WFI));
  }

  @Test
  public void testInjectTimeoutEvent() throws Exception {
    Event injectedEvent = Event.timeout(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/events", eventPayload);

    post.toCompletableFuture().get(1, MINUTES); // block until done

    verify(stateManager).receive(Event.timeout(WFI));
  }

  @Test
  public void shouldFailOnInjectRetryEvent() throws Exception {
    Event injectedEvent = Event.retry(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/events", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void injectEventShouldReturnServerErrorIfRuntimeException() throws Exception {
    assertThat(retryFailureResponse(new RuntimeException("test")), hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
  }

  @Test
  public void injectEventShouldReturnBadRequestOnIllegalArgumentException() throws Exception {
    assertThat(retryFailureResponse(new IllegalArgumentException("badf00d!")), hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void injectEventShouldReturnBadRequestOnIllegalStateException() throws Exception {
    assertThat(retryFailureResponse(new IllegalStateException("badf00d!")), hasStatus(withCode(Status.BAD_REQUEST)));
  }

  private Response<ByteString> retryFailureResponse(Throwable cause) throws Exception {
    doThrow(cause).when(stateManager).receive(any());

    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/retry", eventPayload);

    return post.toCompletableFuture().get();
  }

  @Test
  public void testTriggerShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(Workflow.class));

    final Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(
        TriggerRequest.of(FULL_DAILY_WORKFLOW.id(), "2014-12-31"));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void testTriggeredWorkflowGeneratesTrigger() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);
    final TriggerResponse triggerResponse =
        OBJECT_MAPPER.readValue(response.payload().get().toByteArray(), TriggerResponse.class);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2014-12-31T23:00:00.000Z");
    verify(triggerListener).event(eq(HOURLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
    final Trigger trigger = triggerCaptor.getValue();
    final String triggerId = TriggerUtil.triggerId(trigger);
    assertThat(TriggerUtil.triggerType(trigger), is("adhoc"));
    assertThat(triggerId, startsWith("ad-hoc-cli-"));
    assertThat(triggerResponse.triggerId(), is(triggerId));
  }

  @Test
  public void testTriggerWorkflowInstanceHourly() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2014-12-31T23:00:00.000Z");
    verify(triggerListener).event(eq(HOURLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceDaily() throws Exception {
    when(storage.workflow(DAILY_WORKFLOW.id())).thenReturn(Optional.of(DAILY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(DAILY_WORKFLOW.id(), "2014-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2014-12-31T00:00:00.00Z");
    verify(triggerListener).event(eq(DAILY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceWeekly() throws Exception {
    when(storage.workflow(WEEKLY_WORKFLOW.id())).thenReturn(Optional.of(WEEKLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(WEEKLY_WORKFLOW.id(), "2016-01-03");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2015-12-28T00:00:00.000Z");
    verify(triggerListener).event(eq(WEEKLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerFlyteWorkflowInstance() throws Exception {
    when(storage.workflow(FLYTE_WORKFLOW.id())).thenReturn(Optional.of(FLYTE_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(FLYTE_WORKFLOW.id(), "2014-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2014-12-31T00:00:00.00Z");
    verify(triggerListener).event(eq(FLYTE_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceMissingStorage() throws Exception {
    when(storage.workflow(any())).thenReturn(Optional.empty());

    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("The specified workflow is not"
                                                      + " found in the scheduler")));

    verify(triggerListener, never()).event(any(), any(), any(), any());
  }

  @Test
  public void testTriggerWorkflowInstanceFailingStorage() throws Exception {
    Storage failingStorage = mock(Storage.class);
    when(failingStorage.workflow(any(WorkflowId.class))).thenThrow(new IOException("Error"));

    ServiceHelper serviceHelper = getServiceHelper(stateManager, failingStorage);
    serviceHelper.start();

    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2016-12-31T23");
    ByteString eventPayload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/trigger", eventPayload);
    Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response.status(),
               is(Status.INTERNAL_SERVER_ERROR
                      .withReasonPhrase("An error occurred while retrieving "
                                        + "workflow specifications")));
    verify(triggerListener, never()).event(any(), any(), any(), any());
  }

  @Test
  public void testTriggerWorkflowInstanceUnsupportedSchedule() throws Exception {
    when(storage.workflow(DAILY_WORKFLOW.id())).thenReturn(Optional.of(MONTHLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(MONTHLY_WORKFLOW.id(), "2014-12-01");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2014-12-01T00:00:00.000Z");
    verify(triggerListener).event(eq(MONTHLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceFuture() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot trigger an instance of the future")));

    verify(triggerListener, never()).event(any(), any(), any(), any());
  }

  @Test
  public void testTriggerWorkflowInstanceFutureWithAllowFutureFlag() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    ByteString eventPayload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", BASE + "/trigger?allowFuture=true", eventPayload);

    Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response.status(), is(Status.OK));
    final Instant expectedInstant = Instant.parse("2016-12-31T23:00:00.00Z");
    verify(triggerListener).event(eq(HOURLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceParseDayForHourly() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerParameters expectedParameters = TriggerParameters.zero();
    TriggerRequest toTrigger = TriggerRequest.of(HOURLY_WORKFLOW.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));

    final Instant expectedInstant = Instant.parse("2015-12-31T00:00:00.000Z");
    verify(triggerListener).event(eq(HOURLY_WORKFLOW), triggerCaptor.capture(), eq(expectedInstant), eq(expectedParameters));
  }

  @Test
  public void testTriggerWorkflowInstanceParseFailure() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    TriggerRequest toTrigger = TriggerRequest.of(DAILY_WORKFLOW.id(), "2015");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot parse time parameter 2015 - "
                                                      + "Text '2015' could not be parsed at index 4")));

    verify(triggerListener, never()).event(any(), any(), any(), any());
  }

  @Test
  public void failTriggeringOnUnknownException() throws Exception {
    final Exception exception = new RuntimeException("unknown exception!");
    failtriggeringOnException(DAILY_WORKFLOW, exception, Status.INTERNAL_SERVER_ERROR.withReasonPhrase(exception.getMessage()));
  }

  @Test
  public void failTriggeringOnIllegalArgumentException() throws Exception {
    final IllegalArgumentException exception = new IllegalArgumentException("illegal argument!");
    failtriggeringOnException(DAILY_WORKFLOW, exception, Status.CONFLICT.withReasonPhrase(exception.getMessage()));
  }

  @Test
  public void failTriggeringOnIllegalStateException() throws Exception {
    final IllegalStateException exception = new IllegalStateException("illegal state!");
    failtriggeringOnException(DAILY_WORKFLOW, exception, Status.CONFLICT.withReasonPhrase(exception.getMessage()));
  }

  @Test
  public void failTriggeringAlreadyActiveWorkflowInstance() throws Exception {
    final AlreadyInitializedException exception = new AlreadyInitializedException("already active!");
    failtriggeringOnException(DAILY_WORKFLOW, exception, Status.CONFLICT.withReasonPhrase(
        "This workflow instance is already triggered. Did you want to `retry` running it instead?"));
  }

  @Test
  public void testTriggerMissingImageAndFlyteExecConfig() throws Exception {
    final WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(FULL_DAILY_WORKFLOW.configuration())
            .dockerImage(Optional.empty())
            .flyteExecConf(Optional.empty())
            .build();
    final Workflow workflow = Workflow.create("styx", workflowConfiguration);
    when(storage.workflow(workflow.id())).thenReturn(Optional.of(workflow));
    TriggerRequest toTrigger = TriggerRequest.of(workflow.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Workflow is missing execution "
                                                      + "configuration")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerInvalidWorkflowConfiguration() throws Exception {
    when(workflowValidator.validateWorkflow(DAILY_WORKFLOW)).thenReturn(List.of("bad", "f00d"));

    when(storage.workflow(DAILY_WORKFLOW.id())).thenReturn(Optional.of(DAILY_WORKFLOW));
    TriggerRequest toTrigger = TriggerRequest.of(DAILY_WORKFLOW.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: bad, f00d")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  private void failtriggeringOnException(Workflow wf, Exception e, StatusType preferredStatusType) throws Exception {
    when(storage.workflow(wf.id())).thenReturn(Optional.of(wf));
    TriggerRequest toTrigger = TriggerRequest.of(wf.id(), "2015-12-31");

    doThrow(e).when(triggerListener).event(any(), any(), any(), any());

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(preferredStatusType));
  }
}
