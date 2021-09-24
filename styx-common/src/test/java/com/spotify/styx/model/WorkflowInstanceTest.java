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

package com.spotify.styx.model;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class WorkflowInstanceTest {

  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("foo", "bar");
  private static final String PARAMETER = "baz";
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(WORKFLOW_ID, PARAMETER);

  @Test
  public void create() {
    assertThat(WORKFLOW_INSTANCE.workflowId(), is(WORKFLOW_ID));
    assertThat(WORKFLOW_INSTANCE.parameter(), is(PARAMETER));
  }

  @Test
  public void toKey() {
    assertThat(WORKFLOW_INSTANCE.toKey(), is(WORKFLOW_ID.toKey() + "#" + PARAMETER));
  }

  @Test
  public void toStringShouldReturnKey() {
    assertThat(WORKFLOW_INSTANCE.toString(), is(WORKFLOW_INSTANCE.toKey()));
  }

  @Test
  public void parseKey() {
    assertThat(WorkflowInstance.parseKey(WORKFLOW_INSTANCE.toKey()), is(WORKFLOW_INSTANCE));
  }

  @Test
  public void parseKeyShouldThrowError() {
    assertThrows(
        "Key must contain a hash '#' sign on position > 0", IllegalArgumentException.class,
        () -> WorkflowInstance.parseKey("invalid.workflow.instance.key"));
  }
}
