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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.spotify.styx.util.Json;
import java.io.IOException;
import okio.ByteString;

public class TriggerSerializer {

  private static final TriggerSerializerVisitor SERIALIZER_VISITOR = new TriggerSerializerVisitor();
  private static final ObjectMapper OBJECT_MAPPER = Json.OBJECT_MAPPER;

  public static PersistentTrigger convertTriggerToPersistentTrigger(Trigger trigger) {
    return trigger.accept(SERIALIZER_VISITOR);
  }

  public ByteString serialize(Trigger trigger) {
    try {
      return ByteString.of(OBJECT_MAPPER.writeValueAsBytes(convertTriggerToPersistentTrigger(trigger)));
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public Trigger deserialize(ByteString json) {
    try {
      return OBJECT_MAPPER.readValue(json.toByteArray(), PersistentTrigger.class).toTrigger();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static class TriggerSerializerVisitor implements TriggerVisitor<PersistentTrigger> {

    @Override
    public PersistentTrigger natural() {
      return new PersistentTrigger("natural");
    }

    @Override
    public PersistentTrigger adhoc(String triggerId) {
      return new PersistentTrigger.Adhoc(triggerId);
    }

    @Override
    public PersistentTrigger backfill(String triggerId) {
      return new PersistentTrigger.Backfill(triggerId);
    }

    @Override
    public PersistentTrigger unknown(String triggerId) {
      return new PersistentTrigger.Unknown(triggerId);
    }
  }

  @JsonTypeInfo(use = Id.NAME, visible = true)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = PersistentTrigger.class, name = "natural"),
      @JsonSubTypes.Type(value = PersistentTrigger.Adhoc.class, name = "adhoc"),
      @JsonSubTypes.Type(value = PersistentTrigger.Backfill.class, name = "backfill"),
      @JsonSubTypes.Type(value = PersistentTrigger.Unknown.class, name = "unknown"),
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

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof PersistentTrigger) {
        PersistentTrigger that = (PersistentTrigger) o;
        return (this.type.equals(that.type));
      }
      return false;
    }

    public Trigger toTrigger() {
      switch (type) {
        case "natural":
          return Trigger.natural();

        default:
          throw new IllegalStateException("Trigger type " + type + " not covered by base PersistentTrigger class");
      }
    }

    public static class Adhoc extends PersistentTrigger {

      public final String triggerId;

      @JsonCreator
      public Adhoc(
          @JsonProperty("trigger_id") String triggerId) {
        super("adhoc");
        this.triggerId = triggerId;
      }

      @Override
      public Trigger toTrigger() {
        return Trigger.adhoc(triggerId);
      }

      @Override
      public boolean equals(Object o) {
        if (o == this) {
          return true;
        }
        if (o instanceof Adhoc) {
          Adhoc that = (Adhoc) o;
          return (this.type.equals(that.type))
                 && (this.triggerId.equals(that.triggerId));
        }
        return false;
      }
    }

    public static class Backfill extends PersistentTrigger {

      public final String triggerId;

      @JsonCreator
      public Backfill(
          @JsonProperty("trigger_id") String triggerId) {
        super("backfill");
        this.triggerId = triggerId;
      }

      @Override
      public Trigger toTrigger() {
        return Trigger.backfill(triggerId);
      }

      @Override
      public boolean equals(Object o) {
        if (o == this) {
          return true;
        }
        if (o instanceof Backfill) {
          Backfill that = (Backfill) o;
          return (this.type.equals(that.type))
                 && (this.triggerId.equals(that.triggerId));
        }
        return false;
      }
    }

    public static class Unknown extends PersistentTrigger {

      public final String triggerId;

      @JsonCreator
      public Unknown(
          @JsonProperty("trigger_id") String triggerId) {
        super("unknown");
        this.triggerId = triggerId;
      }

      @Override
      public Trigger toTrigger() {
        return Trigger.unknown(triggerId);
      }

      @Override
      public boolean equals(Object o) {
        if (o == this) {
          return true;
        }
        if (o instanceof Unknown) {
          Unknown that = (Unknown) o;
          return (this.type.equals(that.type))
                 && (this.triggerId.equals(that.triggerId));
        }
        return false;
      }
    }
  }
}
