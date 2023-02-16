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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClassEnforcerTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void shouldAssertNotInstantiable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(ClassEnforcer.class), is(true));
  }

  @Test
  public void shouldAssertNotInstantiableConstructorThrowsUnsupportedOperationException() throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Constructor should throw UnsupportedOperationException: " +
        FailingInstantiable.class.getName());
    ClassEnforcer.assertNotInstantiable(FailingInstantiable.class);
  }

  @Test
  public void shouldAssertNotInstantiableConstructorIsPrivate() throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Default constructor should be private: " +
        PublicConstructorUninstantiable.class.getName());
    ClassEnforcer.assertNotInstantiable(PublicConstructorUninstantiable.class);
  }

  @Test
  public void shouldFailInstantiable() throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Constructor should throw UnsupportedOperationException: " +
        Instantiable.class.getName());
    ClassEnforcer.assertNotInstantiable(Instantiable.class);
  }

  @Test
  public void shouldAssertNotInstantiablWithoutDefaultConstructor()
      throws ReflectiveOperationException {
    exception.expect(NoSuchMethodException.class);
    ClassEnforcer.assertNotInstantiable(Class.class);
  }

  @Test
  public void shouldAssertNotInstantiablWithArgs()
      throws ReflectiveOperationException {
    exception.expect(AssertionError.class);
    exception.expectMessage("Class should only have a single private constructor: " +
        InstantiableWithArgs.class.getName());
    ClassEnforcer.assertNotInstantiable(InstantiableWithArgs.class);
  }

  private static class Instantiable {

  }

  private static class FailingInstantiable {

    private FailingInstantiable() {
      throw new RuntimeException();
    }
  }

  private static class PublicConstructorUninstantiable {

    public PublicConstructorUninstantiable() {
      throw new UnsupportedOperationException();
    }
  }

  private static class InstantiableWithArgs {

    private InstantiableWithArgs() {
      throw new UnsupportedOperationException();
    }

    public InstantiableWithArgs(String ignored) { // NOPMD
    }
  }
}
