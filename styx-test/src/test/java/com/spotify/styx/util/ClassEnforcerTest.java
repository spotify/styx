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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClassEnforcerTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void shouldAssertNotInstantiable() throws ReflectiveOperationException {
    ClassEnforcer.assertNotInstantiable(ClassEnforcer.class);
  }

  @Test
  public void shouldAssertNotInstantiableConstructorThrowsUnsupportedOperationException() throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Class should not be instantiable: " + FailingInstantiable.class.getName());
    ClassEnforcer.assertNotInstantiable(FailingInstantiable.class);
  }

  @Test
  public void shouldAssertInstantiable() throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Class should not be instantiable: " + String.class.getName());
    ClassEnforcer.assertNotInstantiable(String.class);
  }

  @Test
  public void shouldAssertNotInstantiablWithoutDefaultConstructor()
      throws ReflectiveOperationException {
    exception.expect(NoSuchMethodException.class);
    ClassEnforcer.assertNotInstantiable(Class.class);
  }

  private static class FailingInstantiable {

    public FailingInstantiable() {
      throw new NullPointerException();
    }
  }
}
