/*-
 * -\-\-
 * Spotify styx
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

public class ClassEnforcer {

  private ClassEnforcer() {
    throw new UnsupportedOperationException();
  }

  /**
   * Verify cls is not instantiable with default constructor.
   *
   * @param cls the class to verify
   *
   * @throws ReflectiveOperationException if cls does not have a default constructor
   * @throws AssertionError if
   */
  public static boolean assertNotInstantiable(Class<?> cls) throws ReflectiveOperationException {
    if (cls.getDeclaredConstructors().length != 1) {
      throw new AssertionError("Class should only have a single private constructor: " + cls.getName());
    }
    final Constructor<?> constructor = cls.getDeclaredConstructor();
    if (!Modifier.isPrivate(constructor.getModifiers())) {
      throw new AssertionError("Default constructor should be private: " + cls.getName());
    }
    constructor.setAccessible(true);
    try {
      constructor.newInstance();
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        return true;
      }
    }
    throw new AssertionError("Constructor should throw UnsupportedOperationException: " + cls.getName());
  }
}
