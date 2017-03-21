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

package com.spotify.styx.serialization;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests serializing an object to java and back.
 */
public class JsonRoundtripTest {

  private static final Logger LOG = LoggerFactory.getLogger(JsonRoundtripTest.class);

  @Test
  public void testRoundtripSchedule() throws Exception {
    Schedule before = TestData.FULL_DATA_SCHEDULE;
    Schedule after = roundtrip(before, Schedule.class);
    assertThat(after, is(before));
  }

  private static final Map<String, Partitioning> LEGACY_PARTITIONING_TESTS =
      ImmutableMap.<String, Partitioning>builder()
          .put("hourly", Partitioning.HOURS)
          .put("HOURLY", Partitioning.HOURS)
          .put("hOuRlY", Partitioning.HOURS)
          .put("weekly", Partitioning.WEEKS)
          .put("WEEKLY", Partitioning.WEEKS)
          .put("wEeKlY", Partitioning.WEEKS)
          .put("daily", Partitioning.DAYS)
          .put("DAILY", Partitioning.DAYS)
          .put("dAiLy", Partitioning.DAYS)
          .build();

  @Test
  public void testLegacyPartitioningSupport() throws Exception {
    for (Map.Entry<String, Partitioning> testCase : LEGACY_PARTITIONING_TESTS.entrySet()) {
      String json = "{\"id\":\"styx.TestWeekly\",\"schedule\":\"" + testCase.getKey() + "\"}";
      Schedule after = roundtrip(json, Schedule.class);
      assertThat(after.schedule(), is(testCase.getValue()));
    }
  }

  private <T> T roundtrip(T t, Class<? extends T> clazz) throws IOException {
    String json = Json.OBJECT_MAPPER.writeValueAsString(t);
    LOG.debug(json);
    return Json.OBJECT_MAPPER.readValue(json, clazz);
  }

  private <T> T roundtrip(String json, Class<? extends T> clazz) throws IOException {
    LOG.debug(json);
    return Json.OBJECT_MAPPER.readValue(json, clazz);
  }
}
