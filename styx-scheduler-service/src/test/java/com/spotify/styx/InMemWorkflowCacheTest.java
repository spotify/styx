/*-
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

package com.spotify.styx;

import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_ID_2;
import static com.spotify.styx.testdata.TestData.WORKFLOW_WITH_RESOURCES;
import static com.spotify.styx.testdata.TestData.WORKFLOW_WITH_RESOURCES_2;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InMemWorkflowCacheTest {

  private WorkflowCache workflowCache;
  
  private Map<WorkflowId, Workflow> workflows;

  @Mock private Supplier<Map<WorkflowId, Workflow>> supplier;

  @Before
  public void setUp() {
    workflows = ImmutableMap.of(WORKFLOW_ID, WORKFLOW_WITH_RESOURCES,
        WORKFLOW_ID_2, WORKFLOW_WITH_RESOURCES_2);
    when(supplier.get()).thenReturn(workflows);
    workflowCache = new InMemWorkflowCache(supplier);
  }

  @Test
  public void shouldGetAllWorkflows() {
    assertThat(workflowCache.all(), is(workflows));
  }
}
