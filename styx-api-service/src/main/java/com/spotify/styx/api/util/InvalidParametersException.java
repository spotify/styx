package com.spotify.styx.api.util;

public class InvalidParametersException extends RuntimeException {
  public InvalidParametersException(String message) {
    super(message);
  }
}
