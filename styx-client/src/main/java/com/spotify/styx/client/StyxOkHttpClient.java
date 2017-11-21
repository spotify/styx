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

import static com.spotify.styx.api.Api.Version.V3;
import static com.spotify.styx.client.FutureOkHttpClient.APPLICATION_JSON;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.client.auth.GoogleIdTokenAuth;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.util.EventUtil;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.ByteString;

/**
 * Styx OkHttp Client Implementation. In case of API errors, the {@link Throwable} in the returned
 * {@link CompletionStage} will be of kind {@link ApiErrorException}. Other errors will be treated
 * as {@link RuntimeException} instead.
 */
public class StyxOkHttpClient implements StyxClient {

  private static final String STYX_API_VERSION = V3.name().toLowerCase();
  private static final String STYX_CLIENT_VERSION =
      "Styx Client " + StyxOkHttpClient.class.getPackage().getImplementationVersion();

  private final URI apiHost;
  private final GoogleIdTokenAuth auth;
  private final FutureOkHttpClient client;

  public static StyxOkHttpClient create(String apiHost) {
    return new StyxOkHttpClient(FutureOkHttpClient.createDefault(), apiHost);
  }

  StyxOkHttpClient(FutureOkHttpClient client, String apiHost) {
    this(client, apiHost, GoogleIdTokenAuth.ofDefaultCredential());
  }

  StyxOkHttpClient(FutureOkHttpClient client, String apiHost, GoogleIdTokenAuth auth) {
    if (apiHost.contains("://")) {
      this.apiHost = URI.create(apiHost);
    } else {
      this.apiHost = URI.create("https://" + apiHost);
    }
    this.client = Objects.requireNonNull(client, "client");
    this.auth = Objects.requireNonNull(auth, "auth");
  }

  @Override
  public CompletionStage<RunStateDataPayload> activeStates(Optional<String> componentId) {
    final HttpUrl.Builder urlBuilder = getUrlBuilder()
        .addPathSegment("status")
        .addPathSegment("activeStates");
    componentId.ifPresent(id -> urlBuilder.addQueryParameter("component", id));
    final HttpUrl url = urlBuilder.build();
    return executeRequest(new Request.Builder().url(url).build(), RunStateDataPayload.class);
  }

  @Override
  public CompletionStage<List<EventInfo>> eventsForWorkflowInstance(String componentId,
                                                                    String workflowId,
                                                                    String parameter) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("status")
        .addPathSegment("events")
        .addPathSegment(componentId)
        .addPathSegment(workflowId)
        .addPathSegment(parameter)
        .build();
    return executeRequest(
        new Request.Builder().url(url).build(), ObjectNode.class)
        .thenApply(json -> {
          final ArrayNode events = json.withArray("events");
          final ImmutableList.Builder<EventInfo> eventInfos = ImmutableList.builder();
          for (JsonNode eventWithTimestamp : events) {
            final long ts = eventWithTimestamp.get("timestamp").asLong();
            final JsonNode event = eventWithTimestamp.get("event");

            String eventName;
            String eventInfo;
            try {
              Event typedEvent = OBJECT_MAPPER.convertValue(event, Event.class);
              eventName = EventUtil.name(typedEvent);
              eventInfo = EventUtil.info(typedEvent);
            } catch (IllegalArgumentException e) {
              // fall back to just inspecting the json
              eventName = event.get("@type").asText();
              eventInfo = "";
            }

            eventInfos.add(EventInfo.create(ts, eventName, eventInfo));
          }
          return eventInfos.build();
        });
  }

  @Override
  public CompletionStage<Workflow> workflow(String componentId, String workflowId) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("workflows")
        .addPathSegment(componentId)
        .addPathSegment(workflowId)
        .build();
    return executeRequest(new Request.Builder().url(url).build(), Workflow.class);
  }

  @Override
  public CompletionStage<Workflow> createOrUpdateWorkflow(String componentId,
                                                          WorkflowConfiguration workflowConfig) {
    final ByteString payload;
    try {
      payload = serialize(workflowConfig);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("workflows")
        .addPathSegment(componentId)
        .build();
    final Request request = new Request.Builder().url(url)
        .post(RequestBody.create(APPLICATION_JSON, payload)).build();
    return executeRequest(request, Workflow.class);
  }

  @Override
  public CompletionStage<Void> deleteWorkflow(String componentId, String workflowId) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("workflows")
        .addPathSegment(componentId)
        .addPathSegment(workflowId)
        .build();
    return executeRequest(new Request.Builder().url(url).delete().build())
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<WorkflowState> workflowState(String componentId, String workflowId) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("workflows")
        .addPathSegment(componentId)
        .addPathSegment(workflowId)
        .addPathSegment("state")
        .build();
    return executeRequest(new Request.Builder().url(url).build(), WorkflowState.class);
  }

  @Override
  public CompletionStage<Void> triggerWorkflowInstance(String componentId,
                                                       String workflowId,
                                                       String parameter) {
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    try {
      final ByteString payload = serialize(workflowInstance);
      final HttpUrl url = getUrlBuilder()
          .addPathSegment("scheduler")
          .addPathSegment("trigger")
          .build();
      return executeRequest(
          new Request.Builder().url(url)
              .post(RequestBody.create(APPLICATION_JSON, payload)).build())
          .thenApply(response -> null);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Void> haltWorkflowInstance(String componentId,
                                                    String workflowId,
                                                    String parameter) {
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    try {
      final ByteString payload = serialize(Event.halt(workflowInstance));
      final HttpUrl url = getUrlBuilder()
          .addPathSegment("scheduler")
          .addPathSegment("events")
          .build();
      return executeRequest(
          new Request.Builder().url(url)
              .post(RequestBody.create(APPLICATION_JSON, payload)).build())
          .thenApply(response -> null);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Void> retryWorkflowInstance(String componentId,
                                                     String workflowId,
                                                     String parameter) {
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        WorkflowId.create(componentId, workflowId),
        parameter);
    try {
      final ByteString payload = serialize(Event.dequeue(workflowInstance));
      final HttpUrl url = getUrlBuilder()
          .addPathSegment("scheduler")
          .addPathSegment("events")
          .build();
      return executeRequest(
          new Request.Builder().url(url)
              .post(RequestBody.create(APPLICATION_JSON, payload)).build())
          .thenApply(response -> null);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Resource> resourceCreate(String resourceId, int concurrency) {
    try {
      final ByteString payload = serialize(Resource.create(resourceId, concurrency));
      final HttpUrl url = getUrlBuilder()
          .addPathSegment("resources")
          .build();
      return executeRequest(new Request.Builder().url(url)
                                .post(RequestBody.create(APPLICATION_JSON, payload)).build(),
                            Resource.class);
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
  }

  @Override
  public CompletionStage<Resource> resourceEdit(String resourceId, int concurrency) {
    final ByteString payload;
    try {
      payload = serialize(Resource.create(resourceId, concurrency));
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("resources")
        .addPathSegment(resourceId)
        .build();
    return executeRequest(new Request.Builder().url(url)
                              .put(RequestBody.create(APPLICATION_JSON, payload)).build(),
                          Resource.class);
  }

  @Override
  public CompletionStage<Resource> resource(String resourceId) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("resources")
        .addPathSegment(resourceId)
        .build();
    return executeRequest(new Request.Builder().url(url).build(), Resource.class);
  }

  @Override
  public CompletionStage<ResourcesPayload> resourceList() {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("resources")
        .build();
    return executeRequest(new Request.Builder().url(url).build(),
                          ResourcesPayload.class);
  }

  @Override
  public CompletionStage<Backfill> backfillCreate(String componentId, String workflowId,
                                                  String start, String end,
                                                  int concurrency) {
    final ByteString payload;
    try {
      payload = serialize(BackfillInput.create(
          Instant.parse(start), Instant.parse(end), componentId, workflowId, concurrency));
    } catch (JsonProcessingException e) {
      return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
    }
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("backfills")
        .build();
    return executeRequest(new Request.Builder().url(url)
                              .post(RequestBody.create(APPLICATION_JSON, payload)).build(),
                          Backfill.class);
  }

  @Override
  public CompletionStage<Backfill> backfillEditConcurrency(String backfillId, int concurrency) {
    return backfill(backfillId, false).thenCompose(backfillPayload -> {
      final Backfill editedBackfill = backfillPayload.backfill()
          .builder()
          .concurrency(concurrency)
          .build();
      final ByteString payload;
      try {
        payload = serialize(editedBackfill);
      } catch (JsonProcessingException e) {
        return CompletableFutures.exceptionallyCompletedFuture(new RuntimeException(e));
      }
      final HttpUrl url = getUrlBuilder()
          .addPathSegment("backfills")
          .addPathSegment(backfillId)
          .build();
      return executeRequest(new Request.Builder().url(url)
                                .put(RequestBody.create(APPLICATION_JSON, payload)).build(),
                            Backfill.class);
    });
  }

  @Override
  public CompletionStage<Void> backfillHalt(String backfillId) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("backfills")
        .addPathSegment(backfillId)
        .build();
    return executeRequest(new Request.Builder().url(url).delete().build())
        .thenApply(response -> null);
  }

  @Override
  public CompletionStage<BackfillPayload> backfill(String backfillId, boolean includeStatus) {
    final HttpUrl url = getUrlBuilder()
        .addPathSegment("backfills")
        .addPathSegment(backfillId)
        .addQueryParameter("status", Boolean.toString(includeStatus))
        .build();
    return executeRequest(new Request.Builder().url(url).build(), BackfillPayload.class);
  }

  @Override
  public CompletionStage<BackfillsPayload> backfillList(Optional<String> componentId,
                                                        Optional<String> workflowId,
                                                        boolean showAll,
                                                        boolean includeStatus) {
    final HttpUrl.Builder urlBuilder = getUrlBuilder()
        .addPathSegment("backfills");
    componentId.ifPresent(c -> urlBuilder.addQueryParameter("component", c));
    workflowId.ifPresent(w -> urlBuilder.addQueryParameter("workflow", w));
    urlBuilder.addQueryParameter("showAll", Boolean.toString(showAll));
    urlBuilder.addQueryParameter("status", Boolean.toString(includeStatus));
    final HttpUrl url = urlBuilder.build();
    return executeRequest(new Request.Builder().url(url).build(), BackfillsPayload.class);
  }

  private <T> CompletionStage<T> executeRequest(final Request request,
                                                final Class<T> tClass) {
    return executeRequest(request).thenApply(response -> {
      if (response.body() == null) {
        throw new RuntimeException("Expected payload not found");
      } else {
        try {
          return OBJECT_MAPPER.readValue(response.body().bytes(), tClass);
        } catch (IOException e) {
          throw new RuntimeException("Error while reading the received payload: " + e);
        }
      }
    });
  }

  private CompletionStage<Response> executeRequest(final Request request) {
    final Optional<String> authToken;
    try {
      authToken = auth.getToken(this.apiHost.toString());
    } catch (IOException | GeneralSecurityException e) {
      // Credential probably invalid, configured wrongly or the token request failed.
      return CompletableFutures.exceptionallyCompletedFuture(
          new ClientErrorException("Authentication failure: " + e.getMessage(), e));
    }

    final Request.Builder requestBuilder = request.newBuilder()
        .header("User-Agent", STYX_CLIENT_VERSION);

    authToken.ifPresent(token -> requestBuilder.header("Authorization", "Bearer " + token));

    return client.send(requestBuilder.build()).handle((response, e) -> {
      if (e != null) {
        final Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof SocketTimeoutException) {
          throw new ClientErrorException(
              "Connection failed: " + rootCause.getMessage() + ": " + apiHost, e);
        } else {
          throw new ClientErrorException("Request failed: " + request, e);
        }
      } else {
        if (response.isSuccessful()) {
          return response;
        } else {
          final String message = response.code() + " " + response.message();
          throw new ApiErrorException(message, response.code(), authToken.isPresent());
        }
      }
    });
  }

  private HttpUrl.Builder getUrlBuilder() {
    final HttpUrl.Builder builder = new HttpUrl.Builder()
        .scheme(apiHost.getScheme())
        .host(apiHost.getHost())
        .addPathSegment("api")
        .addPathSegment(STYX_API_VERSION);
    if (apiHost.getPort() != -1) {
      builder.port(apiHost.getPort());
    }
    return builder;
  }
}
