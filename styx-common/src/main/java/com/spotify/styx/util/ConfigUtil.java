package com.spotify.styx.util;

import com.typesafe.config.Config;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;


public class ConfigUtil {

  private ConfigUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get a string with a default.
   */
  public static String get(Config config, String path, String defaultValue) {
    return get(config, config::getString, path, defaultValue);
  }

  /**
   * Optionally get a string.
   */
  public static Optional<String> getString(Config config, String path) {
    return get(config, config::getString, path);
  }

  /**
   * Get a list of strings, defaulting to empty.
   */
  public static List<String> getStrings(Config config, String path) {
    return get(config, config::getStringList, path, Collections.emptyList());
  }

  /**
   * Get a value with a default.
   */
  public static <T> T get(Config config, Function<String, T> getter, String path, T defaultValue) {
    return get(config, getter, path).orElse(defaultValue);
  }

  /**
   * Optionally get a configuration value.
   */
  public static <T> Optional<T> get(Config config, Function<String, T> getter, String path) {
    if (!config.hasPath(path)) {
      return Optional.empty();
    }
    return Optional.of(getter.apply(path));
  }
}
