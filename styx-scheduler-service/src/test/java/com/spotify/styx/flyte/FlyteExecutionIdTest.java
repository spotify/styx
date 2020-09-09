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

package com.spotify.styx.flyte;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import com.spotify.styx.flyte.FlyteExecutionId;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class FlyteExecutionIdTest {

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
    final FlyteExecutionId flyteExecutionId = FlyteExecutionId.fromProto(response);
    assertThat(flyteExecutionId.project(), is("flyte-test"));
    assertThat(flyteExecutionId.domain(), is("testing"));
    assertThat(flyteExecutionId.name(), is("test-from-proto-creation"));
    assertThat(flyteExecutionId.toUrn(), is("ex:flyte-test:testing:test-from-proto-creation"));
  }

  @Test
  public void testFromUrn() {
    final FlyteExecutionId flyteExecutionId = FlyteExecutionId.fromUrn("ex:flyte-test:testing:test-from-proto-creation");
    assertThat(flyteExecutionId.project(), is("flyte-test"));
    assertThat(flyteExecutionId.domain(), is("testing"));
    assertThat(flyteExecutionId.name(), is("test-from-proto-creation"));
    assertThat(flyteExecutionId.toUrn(), is("ex:flyte-test:testing:test-from-proto-creation"));
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testFromUrnMalformedString() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Expected 4 parts in URN.");
    FlyteExecutionId.fromUrn("ex:flyte-test:testing");
  }

  @Test
  public void testFromUrlWrongPrefix() {
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Expected URN to start with ex.");
    FlyteExecutionId.fromUrn("lp:flyte-test:testing:test-from-url-wrong-prefix");
  }

  @Test
  public void testProjectCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    FlyteExecutionId.create(null, "testing", "test-null-project")
    );
  }

  @Test
  public void testDomainCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    FlyteExecutionId.create("flyte-test", null, "test-null-domain")
    );
  }

  @Test
  public void testNameCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    FlyteExecutionId.create("flyte-test", "testing", null)
    );
  }
}
