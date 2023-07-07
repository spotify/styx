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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.styx.serialization.Json;
import flyteidl.core.Interface;
import flyteidl.core.Literals;
import flyteidl.core.Types;
import flyteidl.core.Types.LiteralType;
import flyteidl.core.Types.LiteralType.TypeCase;
import flyteidl.core.Types.SimpleType;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class FlyteInputsUtils {

  private static final Set<TypeCase> COMPLEX_TYPES =
      Set.of(TypeCase.COLLECTION_TYPE, TypeCase.MAP_VALUE_TYPE);

  private FlyteInputsUtils() {
    throw new UnsupportedOperationException();
  }

  static Literals.Literal literalOf(String key, String value, Types.LiteralType literalType) {

    switch (literalType.getTypeCase()) {
      case SIMPLE:
        return parseSimpleType(key, value, literalType.getSimple());
      case COLLECTION_TYPE:
        try {
          return parseCollectionType(key, value, literalType.getCollectionType());
        } catch (Exception ex) {
          throw new UnsupportedOperationException(
              String.format(
                  "Collection could not be parsed for input [%s].",
                  key), ex);
        }
      case MAP_VALUE_TYPE:
        try {
          return parserMapType(key, value, literalType.getMapValueType());
        } catch (Exception ex) {
          throw new UnsupportedOperationException(
              String.format(
                  "Map could not be parsed for input [%s].", key), ex);
        }
      default:
        String message =
            String.format(
                "Can't get default value for input [%s]. Only DATETIME/STRING/BOOLEAN/DURATION/INTEGER/FLOAT/MAP/COLLECTION is supported but got [%s]",
                key, literalType.getSimple());

        throw new UnsupportedOperationException(message);
    }
  }

  private static Literals.Literal parserMapType(String key, String value, LiteralType innerType)
      throws JsonProcessingException {
    var mapBuilder = Literals.LiteralMap.newBuilder();

    Map<String, Object> map = Json.YAML_MAPPER.readValue(value, Map.class);
    var containsComplexTypes = COMPLEX_TYPES.contains(innerType.getTypeCase());

    for (var element : map.entrySet()) {
      String result;

      if (containsComplexTypes) {
        result = Json.YAML_MAPPER.writeValueAsString(element.getValue());
      } else {
        result = element.getValue().toString();
      }

      mapBuilder.putLiterals(element.getKey(), literalOf(key, result, innerType));
    }

    return Literals.Literal.newBuilder().setMap(mapBuilder.build()).build();
  }

  private static Literals.Literal parseCollectionType(
      String key, String value, LiteralType innerType) throws JsonProcessingException {
    var collectionBuilder = Literals.LiteralCollection.newBuilder();

    List<Object> list = Json.YAML_MAPPER.readValue(value, List.class);
    var containsComplexTypes = COMPLEX_TYPES.contains(innerType.getTypeCase());

    for (var element : list) {
      String result;

      if (containsComplexTypes) {
        result = Json.YAML_MAPPER.writeValueAsString(element);
      } else {
        result = element.toString();
      }

      collectionBuilder.addLiterals(literalOf(key, result, innerType));
    }

    return Literals.Literal.newBuilder().setCollection(collectionBuilder.build()).build();
  }

  private static Literals.Literal parseSimpleType(String key, String value, SimpleType type) {
    switch (type) {
      case DATETIME:
        return datetimeLiteralOf(value);

      case STRING:
        return stringLiteralOf(value);

      case BOOLEAN:
        return booleanLiteralOf(value);

      case DURATION:
        return durationLiteralOf(value);

      case INTEGER:
        return integerLiteralOf(value);

      case FLOAT:
        return floatLiteralOf(value);

      default:
        String message =
            String.format(
                "Can't get default value for input [%s]. Only DATETIME/STRING/BOOLEAN/DURATION/INTEGER/FLOAT/MAP/COLLECTION is supported but got [%s]",
                key, type);

        throw new UnsupportedOperationException(message);
    }
  }

  static Literals.Literal floatLiteralOf(String value) {
    var primitive =
        Literals.Primitive.newBuilder().setFloatValue(Double.parseDouble(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal integerLiteralOf(String value) {
    var primitive = Literals.Primitive.newBuilder().setInteger(Long.parseLong(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal durationLiteralOf(String value) {
    var duration = Duration.parse(value);
    var primitive =
        Literals.Primitive.newBuilder()
            .setDuration(
                com.google.protobuf.Duration.newBuilder()
                    .setSeconds(duration.getSeconds())
                    .setNanos(duration.getNano())
                    .build())
            .build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal datetimeLiteralOf(String value) {
    var primitive = Literals.Primitive.newBuilder().setDatetime(parseBest(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal booleanLiteralOf(String value) {
    var primitive = Literals.Primitive.newBuilder().setBoolean(Boolean.parseBoolean(value)).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal stringLiteralOf(String value) {
    var primitive = Literals.Primitive.newBuilder().setStringValue(value).build();

    return Literals.Literal.newBuilder()
        .setScalar(Literals.Scalar.newBuilder().setPrimitive(primitive).build())
        .build();
  }

  static Literals.Literal getDefaultValue(
      String key,
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

    throw new UnsupportedOperationException(
        "Can't find default value for launch plan input: " + key);
  }

  private static Optional<String> getMultiKeys(Map<String, String> map, String key1, String key2) {
    return Optional.ofNullable(map.get(key1)).or(() -> Optional.ofNullable(map.get(key2)));
  }

  static Literals.LiteralMap fillParameterInInputs(
      Interface.ParameterMap parameterMap,
      Map<String, String> userDefinedInputs,
      Map<String, String> styxVariables,
      Map<String, String> triggerParams) {

    // Validate that user defined inputs exist in the LaunchPlan
    var lowerNoSnakeParamsKeys = getLowerNoSnakeKeys(parameterMap.getParametersMap());
    var lowerNoSnakeInputKeys = getLowerNoSnakeKeys(userDefinedInputs);
    if (!lowerNoSnakeParamsKeys.containsAll(lowerNoSnakeInputKeys)) {
      var unMatchedInputKeys =
          userDefinedInputs.keySet().stream()
              .filter(
                  key ->
                      !lowerNoSnakeParamsKeys.contains(key.toLowerCase())
                          && !lowerNoSnakeParamsKeys.contains(noSnake(key.toLowerCase())))
              .collect(toList());
      throw new UnsupportedOperationException(
          "Inputs don't correspond with launch plans inputs:" + " " + unMatchedInputKeys);
    }

    var literalMapBuilder = Literals.LiteralMap.newBuilder();

    var multiCaseDefaultValues =
        FlyteInputsUtils.combineMapsWithPreference(
            toMultiCaseMap(userDefinedInputs), toMultiCaseMap(styxVariables));

    var multiCaseTriggerParams = toMultiCaseMap(triggerParams);

    parameterMap
        .getParametersMap()
        .forEach(
            (key, parameter) ->
                literalMapBuilder.putLiterals(
                    key,
                    getDefaultValue(
                        key, parameter, multiCaseDefaultValues, multiCaseTriggerParams)));

    return literalMapBuilder.build();
  }

  private static Set<String> getLowerNoSnakeKeys(Map<String, ?> map) {
    return map.keySet().stream()
        .map(String::toLowerCase)
        .map(FlyteInputsUtils::noSnake)
        .collect(toSet());
  }

  private static Map<String, String> combineMapsWithPreference(
      final Map<String, String> map1, final Map<String, String> map2) {
    Map<String, String> combinedMap = new HashMap<>();
    combinedMap.putAll(map1);
    combinedMap.putAll(map2);
    return Map.copyOf(combinedMap);
  }

  private static Map<String, String> toMultiCaseMap(Map<String, String> map) {
    var multiCaseMap = new HashMap<String, String>();
    map.forEach(
        (varName, value) -> {
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
