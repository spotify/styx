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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowConfiguration;
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

  private static final Map<String, Schedule> LEGACY_SCHEDULE_TESTS = Map.ofEntries(
      Map.entry("hourly", Schedule.HOURS),
      Map.entry("HOURLY", Schedule.HOURS),
      Map.entry("hOuRlY", Schedule.HOURS),
      Map.entry("weekly", Schedule.WEEKS),
      Map.entry("WEEKLY", Schedule.WEEKS),
      Map.entry("wEeKlY", Schedule.WEEKS),
      Map.entry("daily", Schedule.DAYS),
      Map.entry("DAILY", Schedule.DAYS),
      Map.entry("dAiLy", Schedule.DAYS)
  );

  @Test
  public void testRoundtripSchedule() throws Exception {
    WorkflowConfiguration before = TestData.FULL_WORKFLOW_CONFIGURATION;
    WorkflowConfiguration after = roundtrip(before, WorkflowConfiguration.class);
    assertThat(after, is(before));
  }

  @Test
  public void testLegacyScheduleSupport() throws Exception {
    for (Map.Entry<String, Schedule> testCase : LEGACY_SCHEDULE_TESTS.entrySet()) {
      String json = "{\"id\":\"styx.TestWeekly\",\"schedule\":\"" + testCase.getKey() + "\"}";
      WorkflowConfiguration after = roundtrip(json, WorkflowConfiguration.class);
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
