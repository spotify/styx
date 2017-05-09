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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import org.junit.Test;

public class GcpUtilTest {

  private static final GoogleJsonError PERMISSION_DENIED_ERROR = new GoogleJsonError()
      .set("status", "PERMISSION_DENIED");

  @Test
  public void responseIsPermissionDenied() throws Exception {
    final GoogleJsonResponseException permissionDenied = new GoogleJsonResponseException(
        new Builder(403, "Forbidden", new HttpHeaders()), PERMISSION_DENIED_ERROR);
    assertThat(GcpUtil.isPermissionDenied(permissionDenied), is(true));
  }

  @Test
  public void notFoundResponseIsNotPermissionDenied() throws Exception {
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonResponseException(
        new Builder(404, "Not Found", new HttpHeaders()), new GoogleJsonError())), is(false));
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonResponseException(
        new Builder(404, "Not Found", new HttpHeaders()), null)), is(false));
  }

  @Test
  public void errorIsPermissionDenied() throws Exception {
    assertThat(GcpUtil.isPermissionDenied(PERMISSION_DENIED_ERROR), is(true));
  }

  @Test
  public void errorIsNotPermissionDenied() throws Exception {
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonError()), is(false));
    assertThat(GcpUtil.isPermissionDenied(new GoogleJsonError().set("status", "foo failed")), is(false));
  }
}