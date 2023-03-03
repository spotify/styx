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

import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


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

  static Literals.Literal getDefaultValue(String key,
                                          Interface.Parameter parameter,
                                          Map<String, String> extraDefaultInputs,
                                          Map<String, String> triggerParameters) {
    var lowerCaseKey = key.toLowerCase();
    var lowerWithSnakeRemoved = noSnake(lowerCaseKey);

    var triggerParam = getMultiKeys(triggerParameters, lowerCaseKey, lowerWithSnakeRemoved);
    if (triggerParam.isPresent()) {
      return literalOf(key, triggerParam.get(), parameter.getVar().getType());
    }

    var extraDefaultInput = getMultiKeys(extraDefaultInputs, lowerCaseKey, lowerWithSnakeRemoved);
    if (extraDefaultInput.isPresent()) {
      return literalOf(key, extraDefaultInput.get(), parameter.getVar().getType());
    }

    if (parameter.hasDefault()) {
      return parameter.getDefault();
    }

    throw new UnsupportedOperationException("Can't find default value for launch plan input: " + key);
  }

  private static Optional<String> getMultiKeys(Map<String, String> map, String key1, String key2) {
    return Optional.ofNullable(map.get(key1))
        .or(() -> Optional.ofNullable(map.get(key2)));
  }

  static Literals.LiteralMap fillParameterInInputs(
      Interface.ParameterMap parameterMap,
      Map<String, String> userDefinedInputs,
      Map<String, String> styxVariables,
      Map<String, String> triggerParams) {

    // Validate that user defined inputs exist in the LaunchPlan
    var paramsKeysInLowercase = getLowerNoSnakeKeys(parameterMap.getParametersMap());
    var lowercaseInputs = getLowerNoSnakeKeys(userDefinedInputs);
    if (!paramsKeysInLowercase.containsAll(lowercaseInputs)) {
      var unMatchedInputKeys = userDefinedInputs.keySet().stream()
          .filter(key -> !paramsKeysInLowercase.contains(key.toLowerCase())
              && !paramsKeysInLowercase.contains(noSnake(key.toLowerCase())))
          .collect(toList());
      throw new UnsupportedOperationException("Inputs don't correspond with launch plans inputs:"
                                              + " " + unMatchedInputKeys);
    }


    var literalMapBuilder = Literals.LiteralMap.newBuilder();

    var multiCaseDefaultValues = FlyteInputsUtils.combineMapsWithPreference(
        toMultiCaseMap(userDefinedInputs),
        toMultiCaseMap(styxVariables));

    var multiCaseTriggerParams = toMultiCaseMap(triggerParams);

    parameterMap
        .getParametersMap()
        .forEach(
            (key, parameter) -> literalMapBuilder.putLiterals(
                key,
                getDefaultValue(key, parameter, multiCaseDefaultValues, multiCaseTriggerParams)));

    return literalMapBuilder.build();
  }

  private static Set<String> getLowerNoSnakeKeys(Map<String, ?> map) {
    return map.keySet().stream()
        .map(String::toLowerCase)
        .map(FlyteInputsUtils::noSnake)
        .collect(toSet());
  }

  private static Map<String, String> combineMapsWithPreference(
      final Map<String, String> map1,
      final Map<String, String> map2) {
    Map<String, String> combinedMap = new HashMap<>();
    combinedMap.putAll(map1);
    combinedMap.putAll(map2);
    return Map.copyOf(combinedMap);
  }

  private static Map<String, String> toMultiCaseMap(Map<String, String> map) {
    var multiCaseMap = new HashMap<String, String>();
    map.forEach((varName, value) -> {
      String lowerCase = varName.toLowerCase();
      multiCaseMap.put(lowerCase, value);
      multiCaseMap.put(noSnake(lowerCase), value);
    });
    return multiCaseMap;
  }

  private static String noSnake(String str) {
    return str.replaceAll("_", "");
  }
}
