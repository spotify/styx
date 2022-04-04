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

import com.google.common.collect.ImmutableMap;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;

import java.util.Map;
import java.util.TreeMap;


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
    var lowercaseKey = key.toLowerCase();

    final String triggerParam = triggerParameters.get(lowercaseKey);
    if (triggerParam != null) {
      return literalOf(key, triggerParam, parameter.getVar().getType());
    }

    var extraDefaultInput = extraDefaultInputs.get(lowercaseKey);

    if (extraDefaultInput != null) {
      return literalOf(key, extraDefaultInput, parameter.getVar().getType());
    }

    if (parameter.hasDefault()) {
      return parameter.getDefault();
    }

    throw new UnsupportedOperationException("Can't find default value for launch plan input: " + key);
  }

  static Literals.LiteralMap fillParameterInInputs(
      Interface.ParameterMap parameterMap,
      Map<String, String> userDefinedInputs,
      Map<String, String> styxVariables,
      Map<String, String> triggerParams) {

    // Validate that user defined inputs exist in the LaunchPlan
    var paramsKeysInLowercase = parameterMap.getParametersMap().keySet().stream().map(String::toLowerCase).collect(toSet());
    var lowercaseInputs = userDefinedInputs.keySet().stream().map(String::toLowerCase).collect(toSet());
    if (!paramsKeysInLowercase.containsAll(lowercaseInputs)) {
      var unMatchedInputKeys = userDefinedInputs.keySet().stream()
          .filter(key -> !paramsKeysInLowercase.contains(key.toLowerCase()))
          .collect(toList());
      throw new UnsupportedOperationException("Inputs don't correspond with launch plans inputs:"
                                              + " " + unMatchedInputKeys);
    }


    var literalMapBuilder = Literals.LiteralMap.newBuilder();

    var combinedInputsLowerCase = FlyteInputsUtils.combineMapsCaseInsensitiveWithOrder(userDefinedInputs, styxVariables)
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(x -> x.getKey().toLowerCase(), Map.Entry::getValue))
        ;

    var triggerParamsToLowerCase = triggerParams
        .entrySet()
        .stream()
        .collect(ImmutableMap.toImmutableMap(x -> x.getKey().toLowerCase(), Map.Entry::getValue))
        ;

    parameterMap
        .getParametersMap()
        .forEach(
            (key, parameter) -> literalMapBuilder.putLiterals(
                key,
                getDefaultValue(key, parameter, combinedInputsLowerCase, triggerParamsToLowerCase)));

    return literalMapBuilder.build();
  }

  // case shouldnt matter because the case is inhereted from the FlyteLaunchPlan
  public static Map<String, String> combineMapsCaseInsensitiveWithOrder(final Map<String, String> map1,
                                                             final Map<String, String> map2) {
    Map<String, String> inputsMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER); // create case insensitive map to keep user defined casing
    inputsMap.putAll(map1); // First use the fields stored in the flyteExecConf
    inputsMap.putAll(map2); // Then override with the triggeredParams
    return ImmutableMap.copyOf(inputsMap);
  }
}
