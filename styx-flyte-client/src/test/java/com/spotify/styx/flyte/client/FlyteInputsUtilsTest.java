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
import static com.spotify.styx.flyte.client.FlyteInputsUtils.fillingParameterInInputs;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;
import java.time.Instant;
import org.junit.Test;


public class FlyteInputsUtilsTest {
  static final Instant INSTANT = Instant.now();
  static final String PARAMETER = INSTANT.toString();

  @Test
  public void shouldFillingParameterToInputs() {
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

    var inputs = fillingParameterInInputs(parameterMap, PARAMETER);

    assertThat(INSTANT.getEpochSecond(), equalTo(inputs.getLiteralsMap()
        .get(PARAMETER_NAME)
        .getScalar()
        .getPrimitive()
        .getDatetime().getSeconds()));
  }

  @Test
  public void shouldOnlyFillingParameterToInputs() {
    var inputName = "xxx";
    var parameterMap = Interface.ParameterMap.newBuilder()
        .putParameters(inputName, Interface.Parameter.newBuilder()
            .setVar(Interface.Variable.newBuilder()
                .setType(Types.LiteralType.newBuilder().
                    setSimple(Types.SimpleType.DATETIME)
                    .build())
                .build())
            .setDefault(buildLiteralForPartition(PARAMETER))
            .build())
        .build();

    var inputs = fillingParameterInInputs(parameterMap, PARAMETER);

    assertNull(inputs.getLiteralsMap()
        .get(PARAMETER_NAME));
    assertThat(INSTANT.getEpochSecond(), equalTo(inputs.getLiteralsMap()
        .get(inputName)
        .getScalar()
        .getPrimitive()
        .getDatetime().getSeconds()));
  }

  @Test
  public void shouldCreateLiteral() {
    var parameter = buildLiteralForPartition(PARAMETER);
    assertTrue(parameter.hasScalar());
    assertTrue(parameter.getScalar().hasPrimitive());
    assertTrue(parameter.getScalar().getPrimitive().getValueCase() == Literals.Primitive.ValueCase.DATETIME);
    assertThat(INSTANT.getEpochSecond(), equalTo(parameter.getScalar().getPrimitive().getDatetime().getSeconds()));
  }

}
