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

public class ClassEnforcer {

  private ClassEnforcer() {
    throw new UnsupportedOperationException();
  }

  /**
   * Verify cls is not instantiable with default constructor.
   *
   * @param cls the class to verify
   *
   * @return true is the class is not instantiable with default constructor
   *
   * @throws ReflectiveOperationException if cls has not default constructor
   */
  public static boolean verifyNotInstantiable(Class<?> cls) throws ReflectiveOperationException {
    try {
      final Constructor<?> constructor = cls.getDeclaredConstructor();
      constructor.setAccessible(true);
      constructor.newInstance();
      return false;
    } catch (InvocationTargetException e) {
      return e.getCause() instanceof UnsupportedOperationException;
    }
  }
}
