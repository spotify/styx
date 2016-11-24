/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.styx.state;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.ExecutionDescription;
import java.util.Optional;

/**
 * TODO: document.
 */
@AutoValue
public abstract class StateData {

  public static final int DEFAULT_TRIES = 0;
  public static final int DEFAULT_EXIT = -1;
  public static final long DEFAULT_RETRY_MILLIS = 0L;
  public static final double DEFAULT_RETRY_COST = 0.0;

  public abstract int tries();
  public abstract double retryCost();
  public abstract long retryDelayMillis();
  public abstract int lastExit();
  public abstract Optional<String> executionId();
  public abstract Optional<ExecutionDescription> executionDescription();

  public Builder toBuilder() {
    return builder(this);
  }

  /**
   * Returns the default value of a {@link StateData} (algebraic zero).
   */
  public static StateData zero() {
    return builder().build();
  }

  public static Builder builder() {
    return new AutoValue_StateData.Builder()
        .tries(DEFAULT_TRIES)
        .retryCost(DEFAULT_RETRY_COST)
        .retryDelayMillis(DEFAULT_RETRY_MILLIS)
        .lastExit(DEFAULT_EXIT);
  }

  public static Builder builder(StateData stateData) {
    return new AutoValue_StateData.Builder(stateData);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder tries(int tries);
    public abstract Builder retryCost(double cost);
    public abstract Builder retryDelayMillis(long delayMillis);
    public abstract Builder lastExit(int exitCode);
    public abstract Builder executionId(String executionId);
    public abstract Builder executionDescription(ExecutionDescription executionDescription);
    public abstract StateData build();
  }
}
