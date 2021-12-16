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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.FlyteExecConf;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;

import java.util.Map;


public class FlyteInputsUtils {
  private FlyteInputsUtils() {
    throw new UnsupportedOperationException();
  }

  static Literals.Literal literalOf(String key, String value, Types.LiteralType literalType) {
    if (literalType.getTypeCase() != Types.LiteralType.TypeCase.SIMPLE) {
      var message = String.format("Can't get default value for input [%s]. Only DATETIME/STRING/BOOLEAN is supported but got [%s]", key, literalType);

      throw new UnsupportedOperationException(message);
    }

    switch (literalType.getSimple()) {
      case DATETIME:
        return datetimeLiteralOf(value);

      case STRING:
        return stringLiteralOf(value);

      case BOOLEAN:
        return booleanLiteralOf(value);

      default:
        String message = String.format("Can't get default value for input [%s]. Only DATETIME/STRING/BOOLEAN is supported but got [%s]", key, literalType.getSimple());

        throw new UnsupportedOperationException(message);
    }
  }

  static Literals.Literal datetimeLiteralOf(String value) {
    var primitive =
        Literals.Primitive.newBuilder().setDatetime(parseBest(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal booleanLiteralOf(String value) {
    var primitive =
        Literals.Primitive.newBuilder().setBoolean(Boolean.parseBoolean(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal stringLiteralOf(String value) {
    var primitive =
        Literals.Primitive.newBuilder().setStringValue(value).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal getDefaultValue(String key, Interface.Parameter parameter, Map<String, String> extraDefaultInputs) {
    var lowercaseKey = key.toLowerCase();
    var extraDefaultInput = extraDefaultInputs.get(lowercaseKey);

    if (extraDefaultInput != null) {
      return literalOf(key, extraDefaultInput, parameter.getVar().getType());
    }

    if (parameter.hasDefault()) {
      return parameter.getDefault();
    }

    throw new UnsupportedOperationException("Can't find default value for launch plan input: " + key);
  }

  static Literals.LiteralMap fillParameterInInputs(Interface.ParameterMap parameterMap, Map<String, String> extraDefaultInputs) {
    var literalMapBuilder = Literals.LiteralMap.newBuilder();
    var lowercaseExtraDefaultInputs = extraDefaultInputs.entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(x -> x.getKey().toLowerCase(), Map.Entry::getValue));


    var paramsKeysInLowercase = parameterMap.getParametersMap().keySet().stream().map(String::toLowerCase).collect(toSet());
    var lowercaseInputs = extraDefaultInputs.keySet().stream().map(String::toLowerCase).collect(toSet());
    if (!paramsKeysInLowercase.containsAll(lowercaseInputs)) {
      var unMatchedInputKeys = parameterMap.getParametersMap().keySet().stream()
          .filter(key -> !paramsKeysInLowercase.contains(key.toLowerCase()))
          .collect(toList());
      throw new UnsupportedOperationException("Inputs don't correspond with launch plans inputs:"
                                              + " " + unMatchedInputKeys);
    }

    parameterMap
        .getParametersMap()
        .forEach(
            (key, parameter) -> literalMapBuilder.putLiterals(
                key,
                getDefaultValue(key, parameter, lowercaseExtraDefaultInputs)));

    return literalMapBuilder.build();
  }

  public static Map<String, String> computeExtraDefaultInputs(final FlyteExecConf flyteExecConf,
                                                final Map<String, String> styxVariables,
                                                final Map<String, String> triggeredParams) {
      return ImmutableMap.<String, String>builder()
          .putAll(keysToUpperCase(flyteExecConf.inputFields())) // First use the fields stored in the flyteExecConf
          .putAll(keysToUpperCase(styxVariables)) // Then override with the styx variables
          .putAll(keysToUpperCase(triggeredParams)) // Then override with the triggeredParams
          .build();
  }

  private static Map<String, String> keysToUpperCase(Map<String, String> map) {
    return map.entrySet()
        .stream()
        .collect(toUnmodifiableMap(e -> e.getKey().toUpperCase(), Map.Entry::getValue));
  }
}
