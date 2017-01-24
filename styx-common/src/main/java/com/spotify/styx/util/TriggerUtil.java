/*- 
 * -\-\- 
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

import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.TriggerVisitor;

/**
 * Utility for getting information about {@link Trigger}s
 */
public class TriggerUtil {

  private TriggerUtil() {
  }

  public static String name(Trigger trigger) {
    return trigger.accept(TriggerNameVisitor.INSTANCE);
  }

  /**
   * A {@link TriggerVisitor} for extracting the name of a {@link Trigger}.
   */
  private enum TriggerNameVisitor implements TriggerVisitor<String> {
    INSTANCE;

    @Override
    public String natural(String triggerId) {
      return "natural";
    }

    @Override
    public String adhoc(String triggerId) {
      return "adhoc";
    }

    @Override
    public String backfill(String triggerId) {
      return "backfill";
    }

    @Override
    public String unknown(String triggerId) {
      return "unknown";
    }
  }
}
