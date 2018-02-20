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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AggregateStorageTest {

  private static final String COMPONENT = "test-component";

  @Mock BigtableStorage bigtable;
  @Mock DatastoreStorage datastore;
  @Mock WorkflowInstance workflowInstance;
  @Mock PersistentWorkflowInstanceState persistentState;

  private AggregateStorage sut;

  @Before
  public void setUp() throws Exception {
    sut = new AggregateStorage(bigtable, datastore);
  }

  @Test
  public void readActiveWorkflowInstances() throws Exception {
    final Map<WorkflowInstance, PersistentWorkflowInstanceState> activeStates =
        ImmutableMap.of(workflowInstance, persistentState);
    when(datastore.allActiveStates()).thenReturn(activeStates);
    assertThat(sut.readActiveWorkflowInstances(), is(activeStates));
    verify(datastore).allActiveStates();
  }

  @Test
  public void readActiveWorkflowInstance() throws Exception {
    when(datastore.activeState(workflowInstance)).thenReturn(Optional.of(persistentState));
    assertThat(sut.readActiveWorkflowInstance(workflowInstance), is(Optional.of(persistentState)));
    verify(datastore).activeState(workflowInstance);
  }

  @Test
  public void readActiveWorkflowInstancesForComponent() throws Exception {
    final Map<WorkflowInstance, PersistentWorkflowInstanceState> activeStates =
        ImmutableMap.of(workflowInstance, persistentState);
    when(datastore.activeStates(COMPONENT)).thenReturn(activeStates);
    assertThat(sut.readActiveWorkflowInstances(COMPONENT), is(activeStates));
    verify(datastore).activeStates(COMPONENT);
  }

  @Test
  public void writeActiveState() throws Exception {
    sut.writeActiveState(workflowInstance, persistentState);
    verify(datastore).writeActiveState(workflowInstance, persistentState);
  }
}
