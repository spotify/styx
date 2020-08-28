/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.spotify.styx.flyte.FlyteExecution;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FlyteExecutionTest {

  @Test
  public void testFromProto() {
    final ExecutionOuterClass.ExecutionCreateResponse response =
        ExecutionOuterClass.ExecutionCreateResponse
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject("flyte-test")
                .setDomain("testing")
                .setName("test-from-proto-creation")
                .build())
            .build();
    final FlyteExecution flyteExecution = FlyteExecution.fromProto(response);
    assertThat(flyteExecution.getProject(), is("flyte-test"));
    assertThat(flyteExecution.getDomain(), is("testing"));
    assertThat(flyteExecution.getName(), is("test-from-proto-creation"));
    assertThat(flyteExecution.toUrn(), is("ex:flyte-test:testing:test-from-proto-creation"));
  }

  @Test
  public void testFromUrn() {
    final FlyteExecution flyteExecution = FlyteExecution.fromUrn("ex:flyte-test:testing:test-from-proto-creation");
    assertThat(flyteExecution.getProject(), is("flyte-test"));
    assertThat(flyteExecution.getDomain(), is("testing"));
    assertThat(flyteExecution.getName(), is("test-from-proto-creation"));
    assertThat(flyteExecution.toUrn(), is("ex:flyte-test:testing:test-from-proto-creation"));
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testFromUrnMalformedString() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Expected 4 parts in URN.");
    FlyteExecution.fromUrn("ex:flyte-test:testing");
  }

  @Test
  public void testFromUrlWrongPrefix() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Expected URN to start with ex.");
    FlyteExecution.fromUrn("lp:flyte-test:testing:test-from-url-wrong-prefix");
  }
}
