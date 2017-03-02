/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 Spotify AB
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

import static com.spotify.styx.api.ApiVersionTestUtils.ALL_VERSIONS;
import static com.spotify.styx.api.ApiVersionTestUtils.isAtLeast;
import static org.junit.Assume.assumeThat;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.apollo.test.StubClient;
import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A test base class for running a suite of tests against all versions of an api resource.
 */
@RunWith(Parameterized.class)
public abstract class VersionedApiTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> versions() {
    return Stream.of(ALL_VERSIONS)
        .map(v -> new Object[]{v})
        .collect(Collectors.toList());
  }

  @Rule
  public ServiceHelper serviceHelper;

  protected final String basePath;
  protected final Api.Version version;

  protected VersionedApiTest(String basePath, Api.Version version) {
    this(basePath, version, "styx");
  }

  protected VersionedApiTest(String basePath, Api.Version version, String serviceName) {
    this.basePath = basePath;
    this.version = version;
    this.serviceHelper = ServiceHelper.create(this::init, serviceName);
  }

  protected VersionedApiTest(String basePath, Api.Version version, String serviceName, StubClient stubClient) {
    this.basePath = basePath;
    this.version = version;
    this.serviceHelper = ServiceHelper.create(this::init, serviceName, stubClient);
  }

  /**
   * Implement this method for setting up resource routes with a {@link Environment.RoutingEngine}.
   *
   * @param environment The Apollo test environment
   */
  abstract void init(Environment environment);

  /**
   * Test precondition that only runs the calling test case if the version under test is equal or
   * greater than the given version.
   *
   * @param version The version from which the tests are valid
   */
  protected void sinceVersion(Api.Version version) {
    assumeThat(this.version, isAtLeast(version));
  }

  /**
   * Construct a path using the current version prefix and base path.
   *
   * @param path The additional path to add to the base path
   * @return a string that can be used to make api calls
   */
  protected String path(String path) {
    return version.prefix() + basePath + path;
  }

  protected Response<ByteString> awaitResponse(CompletionStage<Response<ByteString>> completionStage)
      throws InterruptedException, ExecutionException, TimeoutException {
    return completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }
}
