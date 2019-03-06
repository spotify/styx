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

package com.spotify.styx.state.handlers;

import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.util.Retrier;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PublisherHandlerTest {

  private static final String COMMIT_SHA = "cc9f6ca490e106ca9324bd34de5e3ad935b91bd6";
  private static final String DOCKER_IMAGE = "busybox:1.1";

  private Publisher publisher;
  private PublisherHandler publisherHandler;

  @Mock private Stats stats;
  @Mock private SequenceEvent sequenceEvent;

  @Before
  public void setUp() throws Exception {
    publisher = mock(Publisher.class);
    publisherHandler = new PublisherHandler(publisher, stats);
  }

  @Test
  public void testPublishesRollingOutStateOnSubmitted() throws Exception {
    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.SUBMITTED,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deploying(WORKFLOW_INSTANCE, executionDescription);
    verify(stats).recordPublishing("deploying", "SUBMITTED");
  }

  @Test
  public void testPublishesDoneStateOnRunning() throws Exception {
    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();

    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.RUNNING,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deployed(WORKFLOW_INSTANCE, executionDescription);
    verify(stats).recordPublishing("deployed", "RUNNING");
  }

  @Test
  public void shouldRetryPublishesOnSubmitted() throws Exception {
    publisherHandler = new PublisherHandler(new FailingPublisher(publisher, 2), stats);

    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.SUBMITTED,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deploying(WORKFLOW_INSTANCE, executionDescription);
    verify(stats, times(2)).recordPublishingError("deploying", "SUBMITTED");
    verify(stats).recordPublishing("deploying", "SUBMITTED");
  }

  @Test
  public void shouldFailEventuallyOnSubmitted() throws Exception {
    doThrow(new IOException()).when(publisher).deploying(any(), any());
    publisherHandler = new PublisherHandler(
        publisher, stats,
        Retrier.builder()
            .maxRetries(1)
            .build());

    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.SUBMITTED,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deploying(WORKFLOW_INSTANCE, executionDescription);
    verify(stats, times(2)).recordPublishingError("deploying", "SUBMITTED");
  }

  @Test
  public void shouldRetryPublishesOnRunning() throws Exception {
    publisherHandler = new PublisherHandler(new FailingPublisher(publisher, 2), stats);

    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.RUNNING,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deployed(WORKFLOW_INSTANCE, executionDescription);
    verify(stats, times(2)).recordPublishingError("deployed", "RUNNING");
    verify(stats).recordPublishing("deployed", "RUNNING");
  }

  @Test
  public void shouldFailEventuallyOnRunning() throws Exception {
    doThrow(new IOException()).when(publisher).deployed(any(), any());
    publisherHandler = new PublisherHandler(
        publisher, stats,
        Retrier.builder()
            .maxRetries(1)
            .build());

    ExecutionDescription executionDescription = ExecutionDescription.builder()
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    RunState runState = RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.RUNNING,
        StateData.newBuilder()
            .executionId("exec1")
            .executionDescription(executionDescription)
            .build());
    publisherHandler.accept(sequenceEvent, runState);

    verify(publisher).deployed(WORKFLOW_INSTANCE, executionDescription);
    verify(stats, times(2)).recordPublishingError("deployed", "RUNNING");
  }

  private class FailingPublisher implements Publisher {

    private final Publisher delegate;
    private final int maxFails;

    private int fails;

    private FailingPublisher(Publisher delegate, int maxFails) {
      this.delegate = delegate;
      this.maxFails = maxFails;
    }

    @Override
    public void deploying(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) throws IOException {
      if (fails++ < maxFails) {
        throw new IOException("failed " + fails);
      }
      delegate.deploying(workflowInstance, executionDescription);
    }

    @Override
    public void deployed(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) throws IOException {
      if (fails++ < maxFails) {
        throw new IOException("failed " + fails);
      }
      delegate.deployed(workflowInstance, executionDescription);
    }

    @Override
    public void close() throws IOException {
      // nop
    }
  }
}
