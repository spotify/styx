package com.spotify.styx.flyte.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class GrpcClientMetadataInterceptor implements ClientInterceptor {

  private final String rpcOriginValue;

  private GrpcClientMetadataInterceptor(String rpcOriginValue) {
    this.rpcOriginValue = rpcOriginValue;
  }

  public static GrpcClientMetadataInterceptor create(String rpcOriginValue) {
    return new GrpcClientMetadataInterceptor(rpcOriginValue);
  }

  static final Metadata.Key<String> RPC_ORIGIN_HEADER_KEY =
      Metadata.Key.of("rpc-origin", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions,
      final Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        Metadata metadata = new Metadata();
        metadata.put(RPC_ORIGIN_HEADER_KEY, rpcOriginValue);
        headers.merge(metadata);
        super.start(responseListener, headers);
      }
    };
  }
}
