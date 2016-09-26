/*
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

import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PublisherHandlerTest {

  private static final String COMMIT_SHA = "cc9f6ca490e106ca9324bd34de5e3ad935b91bd6";
  private static final String DOCKER_IMAGE = "busybox:1.1";

  private Publisher publisher;
  private OutputHandler outputHandler;

  @Before
  public void setUp() throws Exception {
    publisher = mock(Publisher.class);
    outputHandler = new PublisherHandler(publisher);
  }

  @Test
  public void testPublishesRollingOutStateOnSubmitted() throws Exception {
    RunState runState = RunState.newSubmitted(
        WORKFLOW_INSTANCE,
        "exec1",
        ExecutionDescription.create(DOCKER_IMAGE, emptyList(), empty(), Optional.of(COMMIT_SHA)));
    outputHandler.transitionInto(runState);

    verify(publisher).deploying(WORKFLOW_INSTANCE, DOCKER_IMAGE, COMMIT_SHA);
  }

  @Test
  public void testPublishesDoneStateOnRunning() throws Exception {
    RunState runState = RunState.newRunning(
        WORKFLOW_INSTANCE,
        "exec1",
        ExecutionDescription.create("busybox:1.1", emptyList(), empty(), Optional.of(COMMIT_SHA)));
    outputHandler.transitionInto(runState);

    verify(publisher).deployed(WORKFLOW_INSTANCE, DOCKER_IMAGE, COMMIT_SHA);
  }

  @Test
  public void shouldRetryPublishesOnSubmitted() throws Exception {
    outputHandler = new PublisherHandler(new FailingPublisher(publisher, 2));

    RunState runState = RunState.newSubmitted(
        WORKFLOW_INSTANCE,
        "exec1",
        ExecutionDescription.create(DOCKER_IMAGE, emptyList(), empty(), Optional.of(COMMIT_SHA)));
    outputHandler.transitionInto(runState);

    verify(publisher).deploying(WORKFLOW_INSTANCE, DOCKER_IMAGE, COMMIT_SHA);
  }

  @Test
  public void shouldRetryPublishesOnRunning() throws Exception {
    outputHandler = new PublisherHandler(new FailingPublisher(publisher, 2));

    RunState runState = RunState.newRunning(
        WORKFLOW_INSTANCE,
        "exec1",
        ExecutionDescription.create(DOCKER_IMAGE, emptyList(), empty(), Optional.of(COMMIT_SHA)));
    outputHandler.transitionInto(runState);

    verify(publisher).deployed(WORKFLOW_INSTANCE, DOCKER_IMAGE, COMMIT_SHA);
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
    public void deploying(WorkflowInstance workflowInstance, String image, String sha) throws IOException {
      if (fails++ < maxFails) {
        throw new IOException("failed " + fails);
      }
      delegate.deploying(workflowInstance, image, sha);
    }

    @Override
    public void deployed(WorkflowInstance workflowInstance, String image, String sha) throws IOException {
      if (fails++ < maxFails) {
        throw new IOException("failed " + fails);
      }
      delegate.deployed(workflowInstance, image, sha);
    }
  }
}
