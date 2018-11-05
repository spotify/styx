/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.typesafe.config.Config;
import io.norberg.automatter.AutoMatter;
import java.util.Set;
import java.util.stream.Collectors;

@AutoMatter
public interface AuthenticatorConfiguration {

  Set<String> domainWhitelist();
  Set<ResourceId> resourceWhitelist();
  String service();

  static AuthenticatorConfigurationBuilder builder() {
    return new AuthenticatorConfigurationBuilder();
  }

  static AuthenticatorConfiguration fromConfig(Config config, String serviceName) {

    final String domainWhitelistKey = "styx.authentication.domain-whitelist";
    final String resourceWhitelistKey = "styx.authentication.resource-whitelist";

    final AuthenticatorConfigurationBuilder builder = AuthenticatorConfiguration.builder()
        .service(serviceName);

    if (config.hasPath(domainWhitelistKey)) {
      builder.domainWhitelist(config.getStringList(domainWhitelistKey));
    }

    if (config.hasPath(resourceWhitelistKey)) {
      builder.resourceWhitelist(config.getConfigList(resourceWhitelistKey).stream()
          .map(item -> new ResourceId()
              .setType(item.getString("type"))
              .setId(item.getString("id")))
          .collect(Collectors.toSet()));
    }

    return builder.build();
  }
}
