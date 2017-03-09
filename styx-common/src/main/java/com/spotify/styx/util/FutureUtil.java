package com.spotify.styx.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class FutureUtil {

  public static <T> CompletionStage<T> exceptionallyCompletedFuture(final Throwable t) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(t);
    return f;
  }
}
