/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2019 Spotify AB
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

import com.spotify.styx.model.Workflow;
import java.util.List;

public interface WorkflowValidator {

  List<String> validateWorkflow(Workflow workflow);

  static <T extends Comparable<T>> void lowerLimit(List<String> errors, T value, T limit, String message) {
    limit(errors, value.compareTo(limit) < 0, value, limit, message);
  }

  static <T extends Comparable<T>> void upperLimit(List<String> errors, T value, T limit, String message) {
    limit(errors, value.compareTo(limit) > 0, value, limit, message);
  }

  static <T extends Comparable<T>> void limit(List<String> errors, boolean isError, T value, T limit, String message) {
    if (isError) {
      errors.add(message + ": " + value + ", limit = " + limit);
    }
  }
}
