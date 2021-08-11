package com.spotify.styx.flyte;

import io.grpc.ClientInterceptor;
import java.util.Collections;
import java.util.List;

public interface FlyteAdminClientInterceptors {
  FlyteAdminClientInterceptors NOOP = Collections::emptyList;

  List<ClientInterceptor> interceptors();
}
