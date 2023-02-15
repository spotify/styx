/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.storage;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.DatastoreException;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateStorageTest {

  private static final String COMPONENT = "test-component";

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock BigtableStorage bigtable;
  @Mock DatastoreStorage datastore;
  @Mock WorkflowInstance workflowInstance;
  @Mock RunState runState;

  private AggregateStorage sut;

  @Before
  public void setUp() throws Exception {
    sut = new AggregateStorage(bigtable, datastore);
  }

  @After
  public void tearDown() throws Exception {
    sut.close();
  }

  @Test
  public void shouldCloseAggregatedStorage() throws IOException {
    sut.close();
    verify(datastore).close();
    verify(bigtable).close();
  }

  @Test
  public void readActiveWorkflowInstances() throws Exception {
    final Map<WorkflowInstance, RunState> activeStates =
        Map.of(workflowInstance, runState);
    when(datastore.readActiveStates()).thenReturn(activeStates);
    assertThat(sut.readActiveStates(), is(activeStates));
    verify(datastore).readActiveStates();
  }

  @Test
  public void readActiveWorkflowInstance() throws Exception {
    when(datastore.readActiveState(workflowInstance)).thenReturn(Optional.of(runState));
    assertThat(sut.readActiveState(workflowInstance), is(Optional.of(runState)));
    verify(datastore).readActiveState(workflowInstance);
  }

  @Test
  public void readActiveWorkflowInstancesForComponent() throws Exception {
    final Map<WorkflowInstance, RunState> activeStates =
        Map.of(workflowInstance, runState);
    when(datastore.readActiveStates(COMPONENT)).thenReturn(activeStates);
    assertThat(sut.readActiveStates(COMPONENT), is(activeStates));
    verify(datastore).readActiveStates(COMPONENT);
  }

  @Test
  public void writeActiveState() throws Exception {
    sut.writeActiveState(workflowInstance, runState);
    verify(datastore).writeActiveState(workflowInstance, runState);
  }

  @Test
  public void shouldPropagateTransactionFailure() throws IOException {
    var rootCause = new DatastoreException(1, "fail", "error");
    var cause = new DatastoreIOException(rootCause);
    doThrow(cause).when(datastore).runInTransactionWithRetries(any());
    exception.expect(TransactionException.class);
    exception.expectCause(is(rootCause));
    sut.runInTransactionWithRetries(tx -> "foobar");
  }

  @Test
  public void shouldReturnWorkflowWithState() throws IOException {
    sut.workflowWithState(workflowInstance.workflowId());
    verify(datastore).workflowWithState(workflowInstance.workflowId());
  }
}
