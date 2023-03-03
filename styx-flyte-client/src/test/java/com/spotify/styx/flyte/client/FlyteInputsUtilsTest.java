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
import static com.spotify.styx.flyte.client.FlyteInputsUtils.datetimeLiteralOf;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.fillParameterInInputs;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.stringLiteralOf;
import static org.junit.Assert.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import flyteidl.core.Interface;
import flyteidl.core.Literals.Literal;
import flyteidl.core.Literals.Primitive;
import flyteidl.core.Literals.Scalar;
import flyteidl.core.Types;

import flyteidl.core.Types.SimpleType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.jetbrains.annotations.NotNull;
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
        ImmutableMap.of("EXTRA_PARAMETER", "1970-01-01T01"),
        ImmutableMap.of());

    var timestamp = inputs.getLiteralsMap()
        .get("EXTRA_PARAMETER")
        .getScalar()
        .getPrimitive()
        .getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }

  @Test
  public void shouldFillParameterInInputsEvenWhenCamelCase() {
    ZonedDateTime executionTime = ZonedDateTime.of(LocalDate.of(2023, 1, 1),
        LocalTime.of(1, 0), ZoneOffset.UTC);

    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("styxParameter", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .build())
        .putParameters("endpointA", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(SimpleType.STRING)
                    .build())
                .build())
            .build())
        .putParameters("hadesOverride", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(SimpleType.BOOLEAN)
                    .build())
                .build())
            .setDefault(primitiveLiteral(of(false)))
            .build())
        .build();

    var inputs = fillParameterInInputs(
        parameterMap,
        ImmutableMap.of("endpoint_a", "endsong"),
        ImmutableMap.of("STYX_PARAMETER", executionTime.format(DateTimeFormatter.ISO_DATE_TIME)),
        ImmutableMap.of("HADES_OVERRIDE", "TRUE"));

    assertThat(
        inputs.getLiteralsMap()
            .get("styxParameter")
            .getScalar()
            .getPrimitive()
            .getDatetime(),
        equalTo(Timestamps.fromSeconds(executionTime.toEpochSecond()))
    );
    assertThat(
        inputs.getLiteralsMap()
            .get("endpointA")
            .getScalar()
            .getPrimitive()
            .getStringValue(),
        equalTo("endsong")
    );
    assertThat(
        inputs.getLiteralsMap()
            .get("hadesOverride")
            .getScalar()
            .getPrimitive()
            .getBoolean(),
        equalTo(true)
    );
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
        ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
        ImmutableMap.of());

    var timestamp = inputs.getLiteralsMap()
        .get("STYX_PARAMETER")
        .getScalar()
        .getPrimitive()
        .getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }

  @Test
  public void shouldNotFailForMissingTriggerParam() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("OVERWRITE", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setDefault(stringLiteralOf("FALSE"))
            .build())
        .build();

    var inputs = fillParameterInInputs(
        parameterMap,
        ImmutableMap.of(),
        ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
        ImmutableMap.of("NOT_EXISTENT_PARAMETER", "TRUE"));

    var timestamp = inputs.getLiteralsMap()
        .get("OVERWRITE")
        .getScalar()
        .getPrimitive()
        .getStringValue();

    assertThat(timestamp, equalTo("FALSE"));
  }

  @Test
  public void shouldSetTriggerParam() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters("OVERWRITE", Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setDefault(stringLiteralOf("FALSE"))
            .build())
        .build();

    var inputs = fillParameterInInputs(
        parameterMap,
        ImmutableMap.of(),
        ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
        ImmutableMap.of("OVERWRITE", "TRUE"));

    var timestamp = inputs.getLiteralsMap()
        .get("OVERWRITE")
        .getScalar()
        .getPrimitive()
        .getStringValue();

    assertThat(timestamp, equalTo("TRUE"));
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
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of("UNMATCHED", "1970-01-01T01"), ImmutableMap.of(),ImmutableMap.of()));

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
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

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
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

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
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

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
            .setDefault(primitiveLiteral(Primitive.newBuilder()
                .setStringValue("2020-09-15").build()))
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, ImmutableMap.of("PARAMETER", "abc"), ImmutableMap.of(), ImmutableMap.of()));

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

  @NotNull
  private static Primitive of(boolean value) {
    return Primitive.newBuilder().setBoolean(value).build();
  }

  @NotNull
  private static Literal primitiveLiteral(Primitive primitive) {
    return Literal.newBuilder()
        .setScalar(Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }
}
