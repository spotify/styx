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
