/*-
 * -\-\-
 * Spotify Styx Standalone Service
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

package com.spotify.styx;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

import com.spotify.apollo.Environment;
import com.spotify.metrics.core.SemanticMetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxServiceTest {

  @Mock private Environment environment;

  @Mock private SemanticMetricRegistry semanticMetricRegistry;

  @Before
  public void setUp() {
    when(environment.resolve(SemanticMetricRegistry.class)).thenReturn(semanticMetricRegistry);
  }

  @Test
  public void shouldCreateStatsInstance() {
    assertNotNull(StyxService.stats(environment));
  }

  @Test
  public void shouldNotCreateMultipleStatsInstances() {
    assertSame(StyxService.stats(environment), StyxService.stats(environment));
  }
}
