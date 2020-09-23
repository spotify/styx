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

import static com.spotify.styx.util.ParameterUtil.parseBest;
import static flyteidl.core.Types.SimpleType.DATETIME;

import flyteidl.core.Interface;
import flyteidl.core.Literals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlyteInputsUtils {
  static final String PARAMETER_NAME = "styx_parameter";
  private static final Logger LOG = LoggerFactory.getLogger(FlyteInputsUtils.class);

  private FlyteInputsUtils() {
    throw new UnsupportedOperationException();
  }

  static Literals.Literal buildLiteralForPartition(String value) {

    var primitive =
        Literals.Primitive.newBuilder().setDatetime(parseBest(value)).build();
    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.LiteralMap fillParameterInInputs(Interface.ParameterMap parameterMap, String parameter) {
    var literalMapBuilder = Literals.LiteralMap.newBuilder();
    parameterMap
        .getParametersMap()
        .forEach(
            (key, value) -> {
              if (!key.toLowerCase().equals(PARAMETER_NAME)) {
                if (value.hasDefault()) {
                  literalMapBuilder.putLiterals(key, value.getDefault());
                  return;
                }
                var message = "LP inputs must have default values. Missing default value for key:" + key;
                LOG.error(message);
                throw new UnsupportedOperationException(message);
              }
              if (value.getVar().getType().getSimple() != DATETIME) {
                var message = PARAMETER_NAME + " should be of type DATETIME";
                LOG.error(message);
                throw new IllegalArgumentException(message);
              } else {
                var literal = buildLiteralForPartition(parameter);
                literalMapBuilder.putLiterals(key, literal);
              }
            });

    return literalMapBuilder.build();
  }
}
