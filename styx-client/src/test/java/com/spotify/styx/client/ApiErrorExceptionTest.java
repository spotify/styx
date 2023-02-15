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

package com.spotify.styx.client;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;

import java.util.UUID;
import org.junit.Test;

public class ApiErrorExceptionTest {

  private static final String REQUEST_ID = UUID.randomUUID().toString();

  @Test
  public void getMessage() {
    final String message = "fubared: rid=" + REQUEST_ID;
    ApiErrorException exception = new ApiErrorException(message, 4711, true, REQUEST_ID);
    assertThat(exception.getMessage(), is(message));
  }

  @Test
  public void getMessageNoRequestIdInMessage() {
    ApiErrorException exception = new ApiErrorException("fubared!", 4711, true, REQUEST_ID);
    assertThat(exception.getMessage(), is("fubared! (Request ID: " + REQUEST_ID + ")"));
  }

  @Test
  public void getMessageNoRequestId() {
    ApiErrorException exception = new ApiErrorException("fubared!", 4711, true, null);
    assertThat(exception.getMessage(), is("fubared!"));
  }
}
