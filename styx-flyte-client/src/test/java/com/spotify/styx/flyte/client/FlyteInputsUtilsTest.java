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
import static com.spotify.styx.flyte.client.FlyteInputsUtils.literalOf;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.stringLiteralOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import flyteidl.core.Interface;
import flyteidl.core.Literals.Literal;
import flyteidl.core.Literals.Primitive;
import flyteidl.core.Literals.Scalar;
import flyteidl.core.Types;
import flyteidl.core.Types.LiteralType;
import flyteidl.core.Types.SimpleType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class FlyteInputsUtilsTest {

  @Test
  public void shouldFillParameterInInputs() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "EXTRA_PARAMETER",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.DATETIME)
                                    .build())
                            .build())
                    .setDefault(datetimeLiteralOf("2020-09-15"))
                    .build())
            .build();

    var inputs =
        fillParameterInInputs(
            parameterMap,
            ImmutableMap.of(),
            ImmutableMap.of("EXTRA_PARAMETER", "1970-01-01T01"),
            ImmutableMap.of());

    var timestamp =
        inputs.getLiteralsMap().get("EXTRA_PARAMETER").getScalar().getPrimitive().getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }

  @Test
  public void shouldFillParameterInInputsEvenWhenCamelCase() {
    ZonedDateTime executionTime =
        ZonedDateTime.of(LocalDate.of(2023, 1, 1), LocalTime.of(1, 0), ZoneOffset.UTC);

    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "styxParameter",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.DATETIME)
                                    .build())
                            .build())
                    .build())
            .putParameters(
                "endpointA",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                            .build())
                    .build())
            .putParameters(
                "hadesOverride",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(SimpleType.BOOLEAN)
                                    .build())
                            .build())
                    .setDefault(primitiveLiteral(of(false)))
                    .build())
            .build();

    var inputs =
        fillParameterInInputs(
            parameterMap,
            ImmutableMap.of("endpoint_a", "endsong"),
            ImmutableMap.of(
                "STYX_PARAMETER", executionTime.format(DateTimeFormatter.ISO_DATE_TIME)),
            ImmutableMap.of("HADES_OVERRIDE", "TRUE"));

    assertThat(
        inputs.getLiteralsMap().get("styxParameter").getScalar().getPrimitive().getDatetime(),
        equalTo(Timestamps.fromSeconds(executionTime.toEpochSecond())));
    assertThat(
        inputs.getLiteralsMap().get("endpointA").getScalar().getPrimitive().getStringValue(),
        equalTo("endsong"));
    assertThat(
        inputs.getLiteralsMap().get("hadesOverride").getScalar().getPrimitive().getBoolean(),
        equalTo(true));
  }

  @Test
  public void shouldOverrideStyxVariables() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "STYX_PARAMETER",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.DATETIME)
                                    .build())
                            .build())
                    .setDefault(datetimeLiteralOf("2020-09-15"))
                    .build())
            .build();

    var inputs =
        fillParameterInInputs(
            parameterMap,
            ImmutableMap.of("STYX_PARAMETER", "2021-01-01T01"),
            ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
            ImmutableMap.of());

    var timestamp =
        inputs.getLiteralsMap().get("STYX_PARAMETER").getScalar().getPrimitive().getDatetime();

    assertThat(timestamp, equalTo(Timestamps.fromSeconds(3600)));
  }

  @Test
  public void shouldNotFailForMissingTriggerParam() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "OVERWRITE",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.STRING)
                                    .build())
                            .build())
                    .setDefault(stringLiteralOf("FALSE"))
                    .build())
            .build();

    var inputs =
        fillParameterInInputs(
            parameterMap,
            ImmutableMap.of(),
            ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
            ImmutableMap.of("NOT_EXISTENT_PARAMETER", "TRUE"));

    var timestamp =
        inputs.getLiteralsMap().get("OVERWRITE").getScalar().getPrimitive().getStringValue();

    assertThat(timestamp, equalTo("FALSE"));
  }

  @Test
  public void shouldSetTriggerParam() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "OVERWRITE",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.STRING)
                                    .build())
                            .build())
                    .setDefault(stringLiteralOf("FALSE"))
                    .build())
            .build();

    var inputs =
        fillParameterInInputs(
            parameterMap,
            ImmutableMap.of(),
            ImmutableMap.of("STYX_PARAMETER", "1970-01-01T01"),
            ImmutableMap.of("OVERWRITE", "TRUE"));

    var timestamp =
        inputs.getLiteralsMap().get("OVERWRITE").getScalar().getPrimitive().getStringValue();

    assertThat(timestamp, equalTo("TRUE"));
  }

  @Test
  public void shouldComplainWhenUnmatchedInput() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "EXTRA_PARAMETER",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.DATETIME)
                                    .build())
                            .build())
                    .setDefault(datetimeLiteralOf("2020-09-15"))
                    .build())
            .build();

    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                fillParameterInInputs(
                    parameterMap,
                    ImmutableMap.of("UNMATCHED", "1970-01-01T01"),
                    ImmutableMap.of(),
                    ImmutableMap.of()));

    assertThat(
        ex.getMessage(), equalTo("Inputs don't correspond with launch plans inputs: [UNMATCHED]"));
  }

  @Test
  public void shouldThrowExceptionIfInputIsRequired() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "key",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.STRING)
                                    .build())
                            .build())
                    .setRequired(true)
                    .build())
            .build();

    var exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                fillParameterInInputs(
                    parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

    assertThat(
        exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfInputIsNotRequired() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "key",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.STRING)
                                    .build())
                            .build())
                    .setRequired(false)
                    .build())
            .build();

    var exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                fillParameterInInputs(
                    parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

    assertThat(
        exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfInputHasNoDefault() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "key",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.STRING)
                                    .build())
                            .build())
                    .build())
            .build();

    var exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                fillParameterInInputs(
                    parameterMap, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()));

    assertThat(
        exception.getMessage(), equalTo("Can't find default value for launch plan input: key"));
  }

  @Test
  public void shouldThrowExceptionIfParameterTypeIsUnsupported() {
    var parameterMap =
        Interface.ParameterMap.newBuilder()
            .putParameters(
                "PARAMETER",
                Interface.Parameter.newBuilder()
                    .setVar(
                        Interface.Variable.newBuilder()
                            .setType(
                                Types.LiteralType.newBuilder()
                                    .setSimple(Types.SimpleType.BINARY)
                                    .build())
                            .build())
                    .setDefault(
                        primitiveLiteral(
                            Primitive.newBuilder().setStringValue("2020-09-15").build()))
                    .build())
            .build();

    var exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                fillParameterInInputs(
                    parameterMap,
                    ImmutableMap.of("PARAMETER", "abc"),
                    ImmutableMap.of(),
                    ImmutableMap.of()));

    assertThat(
        exception.getMessage(),
        equalTo(
            "Can't get default value for input [PARAMETER]. Only DATETIME/STRING/BOOLEAN/DURATION/INTEGER/FLOAT/MAP/COLLECTION is supported but got [BINARY]"));
  }

  @Test
  public void shouldCreateDatetimeLiteral() {
    var parameter = datetimeLiteralOf("1970-01-02");

    assertThat(
        parameter.getScalar().getPrimitive().getDatetime(),
        equalTo(Timestamp.newBuilder().setSeconds(3600 * 24).setNanos(0).build()));
  }

  @Test
  public void shouldCreateStringLiteral() {
    var parameter = stringLiteralOf("abc");

    assertThat(parameter.getScalar().getPrimitive().getStringValue(), equalTo("abc"));
  }

  @Test
  public void shouldCreateBooleanLiteral() {
    var parameter = booleanLiteralOf("true");

    assertThat(parameter.getScalar().getPrimitive().getBoolean(), equalTo(true));
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

  @Test
  public void shouldCreateBooleanLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey", "true", LiteralType.newBuilder().setSimple(SimpleType.BOOLEAN).build());

    assertThat(parameter.getScalar().getPrimitive().getBoolean(), equalTo(true));
  }

  @Test
  public void shouldCreateStringLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey", "string", LiteralType.newBuilder().setSimple(SimpleType.STRING).build());

    assertThat(parameter.getScalar().getPrimitive().getStringValue(), equalTo("string"));
  }

  @Test
  public void shouldCreateDateTimeLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            OffsetDateTime.parse("2022-01-01T00:00:00Z").toInstant().toString(),
            LiteralType.newBuilder().setSimple(SimpleType.DATETIME).build());

    assertThat(
        parameter.getScalar().getPrimitive().getDatetime().getSeconds(), equalTo(1640995200L));
  }

  @Test
  public void shouldCreateDurationLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "PT20.345S",
            LiteralType.newBuilder().setSimple(SimpleType.DURATION).build());

    assertThat(parameter.getScalar().getPrimitive().getDuration().getSeconds(), equalTo(20L));

    assertThat(parameter.getScalar().getPrimitive().getDuration().getNanos(), equalTo(345000000));
  }

  @Test
  public void shouldCreateIntegerLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey", "101", LiteralType.newBuilder().setSimple(SimpleType.INTEGER).build());

    assertThat(parameter.getScalar().getPrimitive().getInteger(), equalTo(101L));
  }

  @Test
  public void shouldCreateFloatLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey", "101.101", LiteralType.newBuilder().setSimple(SimpleType.FLOAT).build());

    assertThat(parameter.getScalar().getPrimitive().getFloatValue(), equalTo(101.101));
  }

  @Test
  public void shouldCreateStringCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[test,value]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getStringValue)
            .collect(Collectors.toList()),
        equalTo(List.of("test", "value")));
  }

  @Test
  public void shouldCreateIntegerCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[1, 2]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.INTEGER).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getInteger)
            .collect(Collectors.toList()),
        equalTo(List.of(1L, 2L)));
  }

  @Test
  public void shouldCreateFloatCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[1.00, 2.00]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.FLOAT).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getFloatValue)
            .collect(Collectors.toList()),
        equalTo(List.of(1.00, 2.00)));
  }

  @Test
  public void shouldCreateBooleanCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[true, false]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.BOOLEAN).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getBoolean)
            .collect(Collectors.toList()),
        equalTo(List.of(true, false)));
  }

  @Test
  public void shouldCreateDateTimeCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[2022-01-01T00:00:00Z, 2023-01-01T00:00:00Z]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.DATETIME).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getDatetime)
            .map(Timestamp::getSeconds)
            .collect(Collectors.toList()),
        equalTo(List.of(1640995200L, 1672531200L)));
  }

  @Test
  public void shouldCreateDurationCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[PT20S, PT30S]",
            LiteralType.newBuilder()
                .setCollectionType(LiteralType.newBuilder().setSimple(SimpleType.DURATION).build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getDuration)
            .map(Duration::getSeconds)
            .collect(Collectors.toList()),
        equalTo(List.of(20L, 30L)));
  }

  @Test
  public void shouldCreateComplexCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[[test, value], [value]]",
            LiteralType.newBuilder()
                .setCollectionType(
                    LiteralType.newBuilder()
                        .setCollectionType(
                            LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                        .build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(Literal::getCollection)
            .flatMap(literalCollection -> literalCollection.getLiteralsList().stream())
            .map(Literal::getScalar)
            .map(Scalar::getPrimitive)
            .map(Primitive::getStringValue)
            .collect(Collectors.toList()),
        equalTo(List.of("test", "value", "value")));
  }

  @Test
  public void shouldCreateMapCollectionLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "[{test: value}, {value: test}]",
            LiteralType.newBuilder()
                .setCollectionType(
                    LiteralType.newBuilder()
                        .setMapValueType(
                            LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                        .build())
                .build());

    assertThat(
        parameter.getCollection().getLiteralsList().stream()
            .map(l -> l.getMap().getLiteralsMap().entrySet())
            .map(
                s ->
                    s.stream()
                        .map(
                            e ->
                                Map.entry(
                                    e.getKey(),
                                    e.getValue().getScalar().getPrimitive().getStringValue()))
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
            .collect(Collectors.toList()),
        equalTo(List.of(Map.of("test", "value"), Map.of("value", "test"))));
  }

  @Test
  public void shouldCreateStringMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: test}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e ->
                    Map.entry(e.getKey(), e.getValue().getScalar().getPrimitive().getStringValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", "test")));
  }

  @Test
  public void shouldCreateIntegerMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: 1}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.INTEGER).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().getScalar().getPrimitive().getInteger()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", 1L)));
  }

  @Test
  public void shouldCreateFloatMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: 1.00}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.FLOAT).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e -> Map.entry(e.getKey(), e.getValue().getScalar().getPrimitive().getFloatValue()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", 1.00)));
  }

  @Test
  public void shouldCreateBooleanMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: true}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.BOOLEAN).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().getScalar().getPrimitive().getBoolean()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", true)));
  }

  @Test
  public void shouldCreateDateTimeMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: 2022-01-01T00:00:00Z}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.DATETIME).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().getScalar().getPrimitive().getDatetime().getSeconds()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", 1640995200L)));
  }

  @Test
  public void shouldCreateDurationMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: PT20S}",
            LiteralType.newBuilder()
                .setMapValueType(LiteralType.newBuilder().setSimple(SimpleType.DURATION).build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().getScalar().getPrimitive().getDuration().getSeconds()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", 20L)));
  }

  @Test
  public void shouldCreateComplexMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: {innerKey: value}}",
            LiteralType.newBuilder()
                .setMapValueType(
                    LiteralType.newBuilder()
                        .setMapValueType(
                            LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                        .build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().getMap().getLiteralsMap().entrySet().stream()
                            .map(
                                innerE ->
                                    Map.entry(
                                        innerE.getKey(),
                                        innerE
                                            .getValue()
                                            .getScalar()
                                            .getPrimitive()
                                            .getStringValue()))
                            .collect(Collectors.toMap(Entry::getKey, Entry::getValue))))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", Map.of("innerKey", "value"))));
  }

  @Test
  public void shouldCreateListMapLiteralOf() {
    var parameter =
        literalOf(
            "dummyKey",
            "{key: [a,b,c]}",
            LiteralType.newBuilder()
                .setMapValueType(
                    LiteralType.newBuilder()
                        .setCollectionType(
                            LiteralType.newBuilder().setSimple(SimpleType.STRING).build())
                        .build())
                .build());

    assertThat(
        parameter.getMap().getLiteralsMap().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue().getCollection().getLiteralsList().stream()
                            .map(l -> l.getScalar().getPrimitive().getStringValue())
                            .collect(Collectors.toList())))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue)),
        equalTo(Map.of("key", List.of("a", "b", "c"))));
  }
}
