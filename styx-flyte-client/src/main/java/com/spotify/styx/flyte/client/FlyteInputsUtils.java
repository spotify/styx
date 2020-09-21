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

import static flyteidl.core.Types.SimpleType.DATETIME;

import com.google.protobuf.Timestamp;
import com.spotify.styx.util.ParameterUtil;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import java.time.Instant;
import java.time.format.DateTimeParseException;


public class FlyteInputsUtils {
  private static final String PARAMETER_NAME = "parameter";

  private FlyteInputsUtils() {
    throw new UnsupportedOperationException();
  }

  static Literals.Literal buildLiteralForPartition(String value) {

    var primitiveBuilderForString = Literals.Primitive.newBuilder().setDatetime(toTimestamp(value));
    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitiveBuilderForString.build()).build())
        .build();
  }

  static Literals.LiteralMap fillingParameterInInputs(Interface.ParameterMap parameterMap, String parameter) {
    var literalMapBuilder = Literals.LiteralMap.newBuilder();
    parameterMap
        .getParametersMap()
        .forEach(
            (key, value) -> {
              if (key.toLowerCase().equals(PARAMETER_NAME)) {
                if (value.getVar().getType().getSimple() != DATETIME) {
                  literalMapBuilder.putLiterals(key, value.getDefault());
                } else {
                  literalMapBuilder.putLiterals(key, buildLiteralForPartition(parameter));
                }
              } else {
                literalMapBuilder.putLiterals(key, value.getDefault());
              }
            });

    return literalMapBuilder.build();
  }

  private static Timestamp toTimestamp(String value) {
    Instant instant;
    try {
      instant = ParameterUtil.parseDate(value);
    } catch (DateTimeParseException ignored1) {
      try {
        instant = ParameterUtil.parseDateHour(value);
      } catch (DateTimeParseException ignored2) {
        instant = Instant.parse(value);
      }
    }
    return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build();
  }
}
