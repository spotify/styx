package com.spotify.styx.util;

public class ShardNotFoundException extends RuntimeException {

  public ShardNotFoundException(String name) {
    super(name);
  }
}
