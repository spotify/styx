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

package com.spotify.styx.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

public final class TriggerSerializer {

  private static final TriggerSerializerVisitor SERIALIZER_VISITOR = new TriggerSerializerVisitor();

  private TriggerSerializer() {
    // no instantiation
  }

  public static PersistentTrigger convertTriggerToPersistentTrigger(Trigger trigger) {
    return trigger.accept(SERIALIZER_VISITOR);
  }

  private static class TriggerSerializerVisitor implements TriggerVisitor<PersistentTrigger> {

    @Override
    public PersistentTrigger natural() {
      return new PersistentTrigger("natural");
    }

    @Override
    public PersistentTrigger adhoc(String triggerId) {
      return new PersistentTriggerWithId("adhoc", triggerId);
    }

    @Override
    public PersistentTrigger backfill(String triggerId) {
      return new PersistentTriggerWithId("backfill", triggerId);
    }

    @Override
    public PersistentTrigger unknown(String triggerId) {
      return new PersistentTriggerWithId("unknown", triggerId);
    }
  }

  @JsonTypeInfo(use = Id.NAME, visible = true)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = PersistentTrigger.class, name = "natural"),
      @JsonSubTypes.Type(value = PersistentTriggerWithId.class, name = "adhoc"),
      @JsonSubTypes.Type(value = PersistentTriggerWithId.class, name = "backfill"),
      @JsonSubTypes.Type(value = PersistentTriggerWithId.class, name = "unknown"),
      })
  @JsonInclude(Include.NON_ABSENT)
  public static class PersistentTrigger {

    @JsonTypeId
    @JsonProperty("@type") // from Id.NAME
    public final String type;

    @JsonCreator
    PersistentTrigger(
        @JsonProperty("@type") String type) {
      this.type = type;
    }

    public Trigger toTrigger() {
      switch (type) {
        case "natural":
          return Trigger.natural();

        default:
          throw new IllegalStateException("Trigger type " + type + " not covered by base PersistentTrigger class");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PersistentTrigger)) {
        return false;
      }

      PersistentTrigger that = (PersistentTrigger) o;

      return type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return type.hashCode();
    }
  }

  public static class PersistentTriggerWithId extends PersistentTrigger {

    public final String triggerId;

    @JsonCreator
    public PersistentTriggerWithId(
        @JsonProperty("@type") String type,
        @JsonProperty("trigger_id") String triggerId) {
      super(type);
      this.triggerId = triggerId;
    }

    @Override
    public Trigger toTrigger() {
      switch (type) {
        case "adhoc":
          return Trigger.adhoc(triggerId);
        case "backfill":
          return Trigger.backfill(triggerId);
        case "unknown":
          return Trigger.unknown(triggerId);

        default:
          return super.toTrigger();
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PersistentTriggerWithId)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      PersistentTriggerWithId that = (PersistentTriggerWithId) o;

      return triggerId.equals(that.triggerId);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + triggerId.hashCode();
      return result;
    }
  }
}
