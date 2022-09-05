/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.Test;

public class GcpUtilTest {

  private static final GoogleJsonError PERMISSION_DENIED_ERROR = new GoogleJsonError()
      .set("status", "PERMISSION_DENIED");

  private static final GoogleJsonError RESOURCE_EXHAUSTED_ERROR = new GoogleJsonError()
      .set("status", "RESOURCE_EXHAUSTED");

  private static final GoogleJsonError FAILED_PRECONDITION_ERROR = new GoogleJsonError()
      .set("status", "FAILED_PRECONDITION");

  @Test
  public void responseIsPermissionDenied() {
    final Throwable permissionDenied = new GoogleJsonResponseException(
        new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()), PERMISSION_DENIED_ERROR);
    assertThat(GcpUtil.isPermissionDenied(permissionDenied), is(true));
  }

  @Test
  public void notFoundResponseIsNotPermissionDenied() {
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonResponseException(
        new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()), new GoogleJsonError())), is(false));
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonResponseException(
        new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()), null)), is(false));
  }

  @Test
  public void errorIsPermissionDenied() {
    assertThat(GcpUtil.isPermissionDenied(PERMISSION_DENIED_ERROR), is(true));
  }

  @Test
  public void errorIsNotPermissionDenied() {
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonError()), is(false));
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonError().set("status", "foo failed")), is(false));
  }

  @Test
  public void responseIsResourceExhausted() {
    final Throwable resourceExhausted = new GoogleJsonResponseException(
        new HttpResponseException.Builder(429, "Too Many Requests", new HttpHeaders()), RESOURCE_EXHAUSTED_ERROR);
    assertThat(GcpUtil.isResourceExhausted(resourceExhausted), is(true));
  }

  @Test
  public void responseIsFailedPrecondition() {
    final Throwable failedPrecondition = new GoogleJsonResponseException(
        new HttpResponseException.Builder(400, "Precondition check failed", new HttpHeaders()), FAILED_PRECONDITION_ERROR);
    assertThat(GcpUtil.isFailedPrecondition(failedPrecondition), is(true));
  }

  @Test
  public void notFoundResponseIsNotPResourceExhausted() {
    assertThat(GcpUtil.isResourceExhausted(new GoogleJsonResponseException(
        new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()), new GoogleJsonError())), is(false));
    assertThat(GcpUtil.isResourceExhausted(new GoogleJsonResponseException(
        new HttpResponseException.Builder(404, "Not Found", new HttpHeaders()), null)), is(false));
  }

  @Test
  public void errorIsResourceExhausted() {
    assertThat(GcpUtil.isResourceExhausted(RESOURCE_EXHAUSTED_ERROR), is(true));
  }

  @Test
  public void errorIsFailedPrecondition() {
    assertThat(GcpUtil.isFailedPrecondition(FAILED_PRECONDITION_ERROR), is(true));
  }

  @Test
  public void errorIsNotResourceExhausted() {
    assertThat(GcpUtil.isResourceExhausted(new GoogleJsonError()), is(false));
    assertThat(GcpUtil.isResourceExhausted(new GoogleJsonError().set("status", "foo failed")), is(false));
  }
}
