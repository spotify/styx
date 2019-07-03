/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.cli;

class CliExitException extends RuntimeException {

  enum ExitStatus {
    Success(0),
    UnknownError(1),
    ArgumentError(2),
    ClientError(3),
    ApiError(4),
    AuthError(5),
    InputError(6);

    final int code;

    ExitStatus(final int code) {
      this.code = code;
    }
  }


  private final ExitStatus status;

  private CliExitException(ExitStatus status) {
    this.status = status;
  }

  ExitStatus status() {
    return status;
  }

  static CliExitException of(ExitStatus code) {
    return new CliExitException(code);
  }
}
