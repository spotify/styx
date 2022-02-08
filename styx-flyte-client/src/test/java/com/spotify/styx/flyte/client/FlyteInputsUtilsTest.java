/*-
 * -\-\-
 * Spotify Styx Flyte Client
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

package com.spotify.styx.flyte.client;

import static com.spotify.styx.flyte.client.FlyteInputsUtils.booleanLiteralOf;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.combineMapsCaseInsensitiveWithOrder;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.datetimeLiteralOf;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.fillParameterInInputs;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.getExecutionsListFilter;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.stringLiteralOf;
import static org.junit.Assert.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.FlyteIdentifier;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.Test;

public class FlyteInputsUtilsTest {

  @Test
  public void shouldFillParameterInInputs() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("EXTRA_PARAMETER", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .setDefault(datetimeLiteralOf("2020-09-15"))
            .build())
        .build();

    var inputs = fillParameterInInputs(
        parameterMap,
        ImmutableMap.of(),
        ImmutableMap.of("EXTRA_PARAMETER", "1970-01-01T01"));

    var timestamp = inputs.getLiteralsMap()
        .get("EXTRA_PARAMETER")
        .getScalar()
        .getPrimitive()
        .getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }


  @Test
  public void shouldOverrideStyxVariables() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("STYX_PARAMETER", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .setDefault(datetimeLiteralOf("2020-09-15"))
            .build())
        .build();

    var inputs = fillParameterInInputs(
        parameterMap,
        ImmutableMap.of("STYX_PARAMETER", "2021-01-01T01"),
        ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"));

    var timestamp = inputs.getLiteralsMap()
        .get("STYX_PARAMETER")
        .getScalar()
        .getPrimitive()
        .getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }

  @Test
  public void shouldComplainWhenUnmatchedInput() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("EXTRA_PARAMETER", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .setDefault(datetimeLiteralOf("2020-09-15"))
            .build())
        .build();

    var ex = assertThrows(UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of("UNMATCHED", "1970-01-01T01"), ImmutableMap.of()));

    assertThat(ex.getMessage(), equalTo("Inputs don't correspond with launch plans inputs: [UNMATCHED]"));
  }

  @Test
  public void shouldThrowExceptionIfInputIsRequired() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("key", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder()
                    .setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setRequired(true)
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of()));

    assertThat(exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfInputIsNotRequired() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("key", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder()
                    .setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setRequired(false)
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of()));

    assertThat(exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfInputHasNoDefault() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("key", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder()
                    .setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of()));

    assertThat(exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfParameterTypeIsUnsupported() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("PARAMETER", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder()
                    .setSimple(Types.SimpleType.BINARY)
                    .build())
                .build())
            .setDefault(Literals.Literal.newBuilder()
                .setScalar(Literals.Scalar.newBuilder()
                    .setPrimitive(Literals.Primitive.newBuilder()
                        .setStringValue("2020-09-15").build())
                    .build())
                .build())
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of("PARAMETER", "abc"), ImmutableMap.of()));

    assertThat(exception.getMessage(), equalTo("Can't get default value for input [PARAMETER]. Only DATETIME/STRING/BOOLEAN is supported but got [BINARY]"));
  }

  @Test
  public void shouldCreateDatetimeLiteral() {
    var parameter = datetimeLiteralOf("1970-01-02");

    assertThat(
        parameter.getScalar().getPrimitive().getDatetime(),
        equalTo(
            Timestamp.newBuilder()
                .setSeconds(3600 * 24)
                .setNanos(0)
                .build()));
  }

  @Test
  public void shouldCreateStringLiteral() {
    var parameter = stringLiteralOf("abc");

    assertThat(
        parameter.getScalar().getPrimitive().getStringValue(),
        equalTo("abc"));
  }

  @Test
  public void shouldCreateBooleanLiteral() {
    var parameter = booleanLiteralOf("true");

    assertThat(
        parameter.getScalar().getPrimitive().getBoolean(),
        equalTo(true));
  }

  @Test
  public void testInputsCameInOrder() {
    var id =
        FlyteIdentifier.builder().project("project").domain("domain").name("name")
            .version("version").resourceType("LP").build();
    var flyteExecConf = FlyteExecConf.builder().referenceId(id).inputFields("FIELD", "value-flytexecconf").build();
    var inputs = combineMapsCaseInsensitiveWithOrder(flyteExecConf.inputFields(), Map.of());
    assertThat(Map.of("FIELD", "value-flytexecconf"), equalTo(inputs));

    var extraDefaultInputs = Map.of("field", "value-trigger-params");
    inputs = combineMapsCaseInsensitiveWithOrder(flyteExecConf.inputFields(), extraDefaultInputs);
    assertThat(Map.of("FIELD", "value-trigger-params"), equalTo(inputs));
  }

  @Test
  public void testCasingIsPreserved() {
    var id =
        FlyteIdentifier.builder().project("project").domain("domain").name("name")
            .version("version").resourceType("LP").build();
    var flyteExecConf = FlyteExecConf.builder().referenceId(id).inputFields("FiElD", "value-flytexecconf").build();
    var extraDefaultInputs = Map.of("field", "value-trigger-params");
    var inputs = combineMapsCaseInsensitiveWithOrder(flyteExecConf.inputFields(), extraDefaultInputs);
    assertThat(Map.of("FiElD", "value-trigger-params"), equalTo(inputs));
  }

  @Test
  public void testGtExecutionsListFilter() {
    final Instant someTime = LocalDateTime.of(2022, 2, 2, 12, 12, 05)
        .atZone(ZoneOffset.UTC)
        .toInstant();

    final String executionsListFilter = getExecutionsListFilter(someTime);

    assertThat(executionsListFilter,
        equalTo("execution.phase in (RUNNING),execution.started_at>2022-02-01T12:12:05,execution.started_at<2022-02-02T12:09:05"));

  }
}
