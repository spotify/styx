/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.api;

import static com.spotify.styx.api.Authenticator.resourceId;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthenticatorConfigurationTest {

  private static final String DOMAIN_WHITELIST_KEY = "styx.authentication.domain-whitelist";
  private static final String RESOURCE_WHITELIST_KEY = "styx.authentication.resource-whitelist";

  @Mock private Config config;
  
  @Mock private Config resourceType1;

  @Mock private Config resourceType2;

  @Test
  public void shouldBuildFromConfig() {
    when(resourceType1.getString("type")).thenReturn("type1");
    when(resourceType1.getString("id")).thenReturn("1");
    when(resourceType2.getString("type")).thenReturn("type2");
    when(resourceType2.getString("id")).thenReturn("2");

    final List<String> domainWhitelist = ImmutableList.of("foo.com", "bar.com");
    final List<? extends Config> resourceWhitelist = ImmutableList.of(resourceType1, resourceType2);
    
    when(config.hasPath(DOMAIN_WHITELIST_KEY)).thenReturn(true);
    when(config.hasPath(RESOURCE_WHITELIST_KEY)).thenReturn(true);
    
    when(config.getStringList(DOMAIN_WHITELIST_KEY)).thenReturn(domainWhitelist);
    doReturn(resourceWhitelist).when(config).getConfigList(RESOURCE_WHITELIST_KEY);

    final AuthenticatorConfiguration configuration =
        AuthenticatorConfiguration.fromConfig(config, "foo");

    assertThat(configuration.domainWhitelist(), is(ImmutableSet.copyOf(domainWhitelist)));
    assertThat(configuration.resourceWhitelist(), is(ImmutableSet
        .of(resourceId(resourceType1.getString("type"), resourceType1.getString("id")),
            resourceId(resourceType2.getString("type"), resourceType2.getString("id")))));
    assertThat(configuration.service(), is("foo"));
  }
  
  @Test
  public void shouldBuildWithDefaultValues() {
    final AuthenticatorConfiguration configuration =
        AuthenticatorConfiguration.fromConfig(config, "foo");

    assertThat(configuration.domainWhitelist(), is(ImmutableSet.of()));
    assertThat(configuration.resourceWhitelist(), is(ImmutableSet.of()));
    assertThat(configuration.service(), is("foo"));
  }
}
