package com.spotify.styx.util;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class ConfigUtilTest {

  @Test
  public void shouldNotBeInstantiable() throws ReflectiveOperationException {
    ClassEnforcer.assertNotInstantiable(ConfigUtil.class);
  }

  @Test
  public void getWithDefaultShouldReturnValue() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of("foo.bar", "baz"));
    assertThat(ConfigUtil.get(config, "foo.bar", "quux"), is("baz"));
  }

  @Test
  public void getWithDefaultShouldReturnDefault() {
    final Config config = ConfigFactory.empty();
    assertThat(ConfigUtil.get(config, "foo.bar", "quux"), is("quux"));
  }

  @Test
  public void getStringShouldReturnValue() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of("foo.bar", "baz"));
    assertThat(ConfigUtil.getString(config, "foo.bar"), is(Optional.of("baz")));
  }

  @Test
  public void getStringShouldReturnEmpty() {
    final Config config = ConfigFactory.empty();
    assertThat(ConfigUtil.getString(config, "foo.bar"), is(Optional.empty()));
  }

  @Test
  public void getStringsShouldReturnValue() {
    final List<String> value = ImmutableList.of("a", "b");
    final Config config = ConfigFactory.parseMap(ImmutableMap.of("foo.bar", value));
    assertThat(ConfigUtil.getStrings(config, "foo.bar"), is(value));
  }

  @Test
  public void getStringsShouldReturnEmpty() {
    final Config config = ConfigFactory.empty();
    assertThat(ConfigUtil.getStrings(config, "foo.bar"), is(empty()));
  }

  @Test
  public void getWithGetterShouldReturnValue() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of("foo.bar", "baz"));
    assertThat(ConfigUtil.get(config, config::getString, "foo.bar", "quux"), is("baz"));
  }

  @Test
  public void getWithGetterShouldReturnDefault() {
    final Config config = ConfigFactory.empty();
    assertThat(ConfigUtil.get(config, config::getString, "foo.bar", "quux"), is("quux"));
  }

  @Test
  public void getShouldReturnValue() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of("foo.bar", "baz"));
    assertThat(ConfigUtil.get(config, config::getString, "foo.bar"), is(Optional.of("baz")));
  }

  @Test
  public void getShouldReturnEmpty() {
    final Config config = ConfigFactory.empty();
    assertThat(ConfigUtil.get(config, config::getString, "foo.bar"), is(Optional.empty()));
  }
}