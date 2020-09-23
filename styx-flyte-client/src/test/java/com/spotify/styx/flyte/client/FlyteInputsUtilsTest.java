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

import static com.spotify.styx.flyte.client.FlyteInputsUtils.PARAMETER_NAME;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.buildLiteralForPartition;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.fillParameterInInputs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;
import java.time.Instant;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class FlyteInputsUtilsTest {
  static final Instant INSTANT = Instant.now();
  static final String PARAMETER = INSTANT.toString();
  private static final String KEY = "name";
  private static final String EXCEPTION_MESSAGE = "LP inputs must have default values. Missing default value for key:" + KEY;

  @Test
  public void shouldFillParameterInInputs() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(PARAMETER_NAME, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .setDefault(buildLiteralForPartition("2020-09-15"))
            .build())
        .build();

    var inputs = fillParameterInInputs(parameterMap, PARAMETER);

    assertThat(INSTANT.getEpochSecond(), equalTo(inputs.getLiteralsMap()
        .get(PARAMETER_NAME)
        .getScalar()
        .getPrimitive()
        .getDatetime().getSeconds()));
  }

  @Test
  public void shouldOnlyFillParameterInInputs() {
    var inputName = "key";
    var inputValue = "value";
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(inputName, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setDefault(Literals.Literal.newBuilder()
                .setScalar(Literals.Scalar.newBuilder()
                    .setPrimitive(Literals.Primitive.newBuilder().setStringValue(inputValue)
                        .build()).build())
                .build())
            .build())
        .build();

    var inputs = fillParameterInInputs(parameterMap, PARAMETER);

    assertNull(inputs.getLiteralsMap()
        .get(PARAMETER_NAME));
    assertThat(inputValue, equalTo(inputs.getLiteralsMap()
        .get(inputName)
        .getScalar()
        .getPrimitive()
        .getStringValue()));
  }

  @Test
  public void shouldThrowExceptionIfInputIsRequired() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(KEY, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setRequired(true)
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, PARAMETER)
    );
    assertThat(exception.getMessage(), equalTo(EXCEPTION_MESSAGE));
  }

  @Test
  public void shouldThrowExceptionIfInputIsNotRequired() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(KEY, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .setRequired(false)
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, PARAMETER)
    );
    assertThat(exception.getMessage(), equalTo(EXCEPTION_MESSAGE));
  }

  @Test
  public void shouldThrowExceptionIfInputHasNoDefault() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(KEY, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
                    .build())
                .build())
            .build())
        .build();

    var exception = assertThrows(
        UnsupportedOperationException.class,
        () -> fillParameterInInputs(parameterMap, PARAMETER)
    );

    assertThat(exception.getMessage(), equalTo(EXCEPTION_MESSAGE));
  }

  @Test
  public void shouldThrowExceptionIfParameterHasWrongType() {
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(PARAMETER_NAME, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.STRING)
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
        IllegalArgumentException.class,
        () -> fillParameterInInputs(parameterMap, PARAMETER)
    );

    assertThat(exception.getMessage(), equalTo(PARAMETER_NAME + " should be of type DATETIME"));
  }

  @Test
  public void shouldCreateLiteral() {
    var parameter = buildLiteralForPartition(PARAMETER);
    assertTrue(parameter.hasScalar());
    assertTrue(parameter.getScalar().hasPrimitive());
    assertTrue(parameter.getScalar().getPrimitive().getValueCase() == Literals.Primitive.ValueCase.DATETIME);
    assertThat(INSTANT.getEpochSecond(), equalTo(parameter.getScalar().getPrimitive().getDatetime().getSeconds()));
  }

  @Test
  @Parameters({
      "2016-01-19, 2016-01-19T00:00:00Z",
      "2016-01-19T09, 2016-01-19T09:00:00Z",
      "2016-01, 2016-01-01T00:00:00Z",
      "2016, 2016-01-01T00:00:00Z",
      "2016-01-19T09:11:00Z, 2016-01-19T09:11:00Z",
      "2016-01-19T09:11:01Z, 2016-01-19T09:11:01Z",
  })
  public void shouldConvertStringToTimeStamp(String string, String timestamp) {
    long expected = Instant.parse(timestamp).getEpochSecond();
    long result = FlyteInputsUtils.toTimestamp(string).getSeconds();
    assertEquals(result, expected);
  }
}
