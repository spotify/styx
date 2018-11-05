package com.spotify.styx;

import com.spotify.styx.api.Authenticator;
import com.spotify.styx.api.AuthenticatorFactory;
import java.util.Set;

class SingletonAuthenticatorFactory implements AuthenticatorFactory {

  private final AuthenticatorFactory factory;
  private Authenticator authenticator;

  SingletonAuthenticatorFactory(AuthenticatorFactory factory) {
    this.factory = factory;
  }

  @Override
  public synchronized Authenticator apply(Set<String> domainWhitelist, String service) {
    if (authenticator == null) {
      authenticator = factory.apply(domainWhitelist, service);
    }
    return authenticator;
  }
}
