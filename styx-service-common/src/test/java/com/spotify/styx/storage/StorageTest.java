/*
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.storage;

import static org.junit.Assert.fail;

import com.spotify.styx.model.Workflow;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StorageTest {

  @Mock Storage sut;
  @Mock Workflow workflow;

  @Test
  public void shouldBePossibleToThrowUserDefinedExceptionInFunctionBody() {
    // Note that this is a compile-time test. The transaction body is not actually executed, we're
    // just verifying that the type system allows this code to compile.
    try {
      sut.runInTransactionWithRetries(tx -> {
        switch (ThreadLocalRandom.current().nextInt()) {
          case 0:
            throw new FooException();
          case 1:
            return tx.store(workflow);
          default:
            return "foo";
        }
      });
    } catch (IOException | FooException ignore) {
      fail(); // Satisfy static analysis rule requiring assert or fail in junit tests

      // The throws declaration of the above lambda should be inferred to FooException, not the
      // generic Exception. This allows a try/catch wrapping the runInTransaction call to catch
      // a user defined checked exception type to communicate a business logic exception without
      // having to do a generic catch Exception.
    }
  }

  class FooException extends Exception {
  }
}
