/*-
 * -\-\-
 * styx-client
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

package com.spotify.styx.client;

import static com.spotify.styx.client.FutureOkHttpClient.forUri;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.client.auth.GoogleIdTokenAuth;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.TriggerRequest;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.EventUtil;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import okhttp3.HttpUrl.Builder;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Styx OkHttp Client Implementation. In case of API errors, the {@link Throwable} in the returned
 * {@link CompletionStage} will be of kind {@link ApiErrorException}. Other errors will be treated
 * as {@link RuntimeException} instead.
 */
class StyxOkHttpClient implements StyxClient {

  private static final Logger LOG = LoggerFactory.getLogger(StyxOkHttpClient.class);

  static final String STYX_API_VERSION = Api.Version.V3.name().toLowerCase();

  private static final String STYX_CLIENT_VERSION =
      "Styx Client " + StyxOkHttpClient.class.getPackage().getImplementationVersion();

  private final URI apiHost;
  private final FutureOkHttpClient client;
  private final GoogleIdTokenAuth auth;

  private StyxOkHttpClient(String apiHost, FutureOkHttpClient client, GoogleIdTokenAuth auth) {
    if (apiHost.contains("://")) {
      this.apiHost = URI.create(apiHost);
    } else {
      this.apiHost = URI.create("https://" + apiHost);
    }
    this.client = Objects.requireNonNull(client, "client");
    this.auth = Objects.requireNonNull(auth, "auth");
  }

  public static StyxClient create(String apiHost) {
    return create(apiHost, FutureOkHttpClient.createDefault(), GoogleIdTokenAuth.ofDefaultCredential());
  }

  public static StyxClient create(String apiHost, OkHttpClient client) {
    return create(apiHost, FutureOkHttpClient.create(client), GoogleIdTokenAuth.ofDefaultCredential());
  }

  static StyxClient create(String apiHost, FutureOkHttpClient client, GoogleIdTokenAuth auth) {
    return new StyxOkHttpClient(apiHost, client, auth);
  }

  @Override
  public CompletionStage<RunStateDataPayload> activeStates(Optional<String> componentId) {
    final Builder url = urlBuilder("status", "activeStates");
    componentId.ifPresent(id -> url.addQueryParameter("component", id));
    return execute(forUri(url), RunStateDataPayload.class);
  }

  @Override
  public CompletionStage<List<EventInfo>> eventsForWorkflowInstance(String componentId,
                                                                    String workflowId,
                                                                    String parameter) {
    return execute(forUri(urlBuilder("status", "events", componentId, workflowId, parameter)))
        .thenApply(response -> {
          final JsonNode jsonNode;
          try (final ResponseBody responseBody = response.body()) {
            jsonNode = Json.OBJECT_MAPPER.readTree(responseBody.bytes());
          } catch (IOException e) {
            throw new RuntimeException("Invalid json returned from API", e);
          }

          if (!jsonNode.isObject()) {
            throw new RuntimeException("Unexpected json returned from API");
          }

          final ArrayNode events = ((ObjectNode) jsonNode).withArray("events");

          return StreamSupport.stream(events.spliterator(), false)
              .map(eventWithTimestamp -> {
                final long ts = eventWithTimestamp.get("timestamp").asLong();
                final JsonNode event = eventWithTimestamp.get("event");

                try {
                  final Event typedEvent = Json.OBJECT_MAPPER.convertValue(event, Event.class);
                  return EventInfo.create(ts, EventUtil.name(typedEvent), EventUtil.info(typedEvent));
                } catch (IllegalArgumentException e) {
                  // fall back to just inspecting the json
                  return EventInfo.create(ts, event.get("@type").asText(), "");
                }
              })
              .collect(Collectors.toList());
        });
  }

  @Override
  public CompletionStage<Workflow> workflow(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId)), Workflow.class);
  }

  @Override
  public CompletionStage<List<Workflow>> workflows() {
    return execute(forUri(urlBuilder("workflows")), Workflow[].class)
        .thenApply(Arrays::asList);
  }

  @Override
  public CompletionStage<Workflow> createOrUpdateWorkflow(String componentId, WorkflowConfiguration workflowConfig) {
    return execute(forUri(urlBuilder("workflows", componentId), "POST", workflowConfig),
                   Workflow.class);
  }

  @Override
  public CompletionStage<Void> deleteWorkflow(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId), "DELETE"))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<WorkflowState> workflowState(String componentId, String workflowId) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "state")),
                   WorkflowState.class);
  }

  @Override
  public CompletionStage<WorkflowInstanceExecutionData> workflowInstanceExecutions(String componentId,
                                                                                   String workflowId,
                                                                                   String parameter) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "instances", parameter)),
                   WorkflowInstanceExecutionData.class);
  }

  @Override
  public CompletionStage<WorkflowState> updateWorkflowState(String componentId, String workflowId,
                                                            WorkflowState workflowState) {
    return execute(forUri(urlBuilder("workflows", componentId, workflowId, "state"), "PATCH", workflowState),
                   WorkflowState.class);
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId, String workflowId,
      String parameter) {
    return triggerWorkflowInstance(componentId, workflowId, parameter, TriggerParameters.zero());
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId,
                                                       String workflowId,
                                                       String parameter,
                                                       TriggerParameters triggerParameters) {
    final TriggerRequest triggerRequest =
        TriggerRequest.of(WorkflowId.create(componentId, workflowId), parameter, triggerParameters);
    return execute(forUri(urlBuilder("scheduler", "trigger"), "POST", triggerRequest))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Void> haltWorkflowInstance(String componentId,
                                                    String workflowId,
                                                    String parameter) {
    final Builder url = urlBuilder("scheduler", "halt");
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    return execute(forUri(url, "POST", workflowInstance))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Void> retryWorkflowInstance(String componentId,
                                                     String workflowId,
                                                     String parameter) {
    final Builder url = urlBuilder("scheduler", "retry");
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    return execute(forUri(url, "POST", workflowInstance))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<Resource> resourceCreate(String resourceId, int concurrency) {
    final Resource resource = Resource.create(resourceId, concurrency);
    return execute(forUri(urlBuilder("resources"), "POST", resource),
                   Resource.class);
  }

  @Override
  public CompletionStage<Resource> resourceEdit(String resourceId, int concurrency) {
    final Resource resource = Resource.create(resourceId, concurrency);
    return execute(forUri(urlBuilder("resources", resourceId), "PUT", resource),
                   Resource.class);
  }

  @Override
  public CompletionStage<Resource> resource(String resourceId) {
    final Builder url = urlBuilder("resources", resourceId);
    return execute(forUri(url), Resource.class);
  }

  @Override
  public CompletionStage<ResourcesPayload> resourceList() {
    final Builder url = urlBuilder("resources");
    return execute(forUri(url), ResourcesPayload.class);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency) {
    return backfillCreate(componentId, workflowId, start, end, concurrency, null);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency,
                                                  String description) {
    final BackfillInput backfill = BackfillInput.newBuilder()
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .component(componentId)
        .workflow(workflowId)
        .concurrency(concurrency)
        .description(Optional.ofNullable(description))
        .build();
    return backfillCreate(backfill);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(BackfillInput backfill) {
    return execute(forUri(urlBuilder("backfills"), "POST", backfill), Backfill.class);
  }

  @Override
  public CompletionStage<Backfill> backfillEditConcurrency(String backfillId, int concurrency) {
    final EditableBackfillInput editableBackfillInput = EditableBackfillInput.newBuilder()
        .id(backfillId)
        .concurrency(concurrency)
        .build();
    final Builder url = urlBuilder("backfills", backfillId);
    return execute(forUri(url, "PUT", editableBackfillInput), Backfill.class);
  }

  @Override
  public CompletionStage<Void> backfillHalt(String backfillId) {
    return execute(forUri(urlBuilder("backfills", backfillId), "DELETE"))
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<BackfillPayload> backfill(String backfillId, boolean includeStatus) {
    final Builder url = urlBuilder("backfills", backfillId);
    url.addQueryParameter("status", Boolean.toString(includeStatus));
    return execute(forUri(url), BackfillPayload.class);
  }

  @Override
  public CompletionStage<BackfillsPayload> backfillList(Optional<String> componentId,
                                                        Optional<String> workflowId,
                                                        boolean showAll,
                                                        boolean includeStatus) {
    final Builder url = urlBuilder("backfills");
    componentId.ifPresent(c -> url.addQueryParameter("component", c));
    workflowId.ifPresent(w -> url.addQueryParameter("workflow", w));
    url.addQueryParameter("showAll", Boolean.toString(showAll));
    url.addQueryParameter("status", Boolean.toString(includeStatus));
    return execute(forUri(url), BackfillsPayload.class);
  }

  private <T> CompletionStage<T> execute(Request request, Class<T> tClass) {
    return execute(request).thenApply(response -> {
      try (final ResponseBody responseBody = response.body()) {
        return Json.OBJECT_MAPPER.readValue(responseBody.bytes(), tClass);
      } catch (IOException e) {
        throw new RuntimeException("Error while reading the received payload: " + e.getMessage(), e);
      }
    });
  }

  private Request decorateRequest(Request request, String requestId, Optional<String> authToken) {
    final Request.Builder builder = request
        .newBuilder()
        .addHeader("User-Agent", STYX_CLIENT_VERSION)
        .addHeader("X-Request-Id", requestId);
    authToken.ifPresent(t -> builder.addHeader("Authorization", "Bearer " + t));
    return builder.build();
  }

  private CompletionStage<Response> execute(Request request) {
    final Optional<String> authToken;
    try {
      authToken = auth.getToken(apiHost.toString());
    } catch (IOException | GeneralSecurityException e) {
      // Credential probably invalid, configured wrongly or the token request failed.
      throw new ClientErrorException("Authentication failure: " + e.getMessage(), e);
    }
    final String requestId = UUID.randomUUID().toString().replace("-", "");  // UUID with no dashes, easier to deal with
    return client.send(decorateRequest(request, requestId, authToken)).handle((response, e) -> {
      if (e != null) {
        throw new ClientErrorException("Request failed: " + request.method() + " " + request.url(), e);
      } else {
        final String effectiveRequestId;
        final String responseRequestId = response.headers().get("X-Request-Id");
        if (responseRequestId != null && !responseRequestId.equals(requestId)) {
          // If some proxy etc dropped our request ID header, we might get another one back.
          effectiveRequestId = responseRequestId;
          LOG.warn("Request ID mismatch: '{}' != '{}'", requestId, responseRequestId);
        } else {
          effectiveRequestId = requestId;
        }
        if (!response.isSuccessful()) {
          throw new ApiErrorException(response.code() + " " + response.message(), response.code(),
                                      authToken.isPresent(), effectiveRequestId);
        }
        return response;
      }
    });
  }

  private Builder urlBuilder(String... pathSegments) {
    final Builder builder = new Builder()
        .scheme(apiHost.getScheme())
        .host(apiHost.getHost())
        .addPathSegment("api")
        .addPathSegment(STYX_API_VERSION);
    Arrays.stream(pathSegments).forEach(builder::addPathSegment);
    if (apiHost.getPort() != -1) {
      builder.port(apiHost.getPort());
    }
    return builder;
  }

  @Override
  public void close() {
    client.close();
  }
}
