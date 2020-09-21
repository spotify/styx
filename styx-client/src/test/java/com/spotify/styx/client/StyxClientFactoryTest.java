/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxClientFactoryTest {

  private static final String API_HOST = "https://example.com";

  @Mock
  private GoogleCredentials googleCredentials;

  @Test
  public void createWithHost() {
    var client = StyxClientFactory.create(API_HOST);
    assertThat(client, notNullValue());
  }

  @Test
  public void testCreateWithHostAndCredentials() {
    var client = StyxClientFactory.create(API_HOST, googleCredentials);
    assertThat(client, notNullValue());
  }

  @Test
  public void testCreateWithHostAndClient() {
    var client = StyxClientFactory.create(API_HOST, StyxClientFactory.defaultOkHttpClient());
    assertThat(client, notNullValue());
  }

  @Test
  public void testCreateWithHostClientAndCredentials() {
    var client = StyxClientFactory.create(API_HOST, StyxClientFactory.defaultOkHttpClient(), googleCredentials);
    assertThat(client, notNullValue());
  }
}
