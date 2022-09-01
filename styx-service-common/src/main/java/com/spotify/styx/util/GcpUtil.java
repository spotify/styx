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

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import java.util.Optional;
import java.util.function.Predicate;

public class GcpUtil {

  private static final int BAD_REQUEST = 400;
  private static final int FORBIDDEN = 403;
  private static final int TOO_MANY_REQUEST_CODE = 429;

  private GcpUtil() {
    throw new UnsupportedOperationException();
  }

  public static boolean isPermissionDenied(Throwable t) {
    return t instanceof GoogleJsonResponseException
        && isPermissionDenied((GoogleJsonResponseException) t);
  }

  public static boolean isPermissionDenied(GoogleJsonResponseException e) {
    return matchesCodeAndError(e,  FORBIDDEN, GcpUtil::isPermissionDenied);
  }

  public static boolean isPermissionDenied(GoogleJsonError error) {
    return matchesStatus(error, "PERMISSION_DENIED");
  }

  public static boolean isResourceExhausted(Throwable t) {
    return t instanceof GoogleJsonResponseException
           && isResourceExhausted((GoogleJsonResponseException) t);
  }

  public static boolean isResourceExhausted(GoogleJsonResponseException e) {
    return matchesCodeAndError(e, TOO_MANY_REQUEST_CODE,
        error -> matchesStatus(error, "RESOURCE_EXHAUSTED"));
  }

  public static boolean isResourceExhausted(GoogleJsonError e) {
    return matchesStatus(e, "RESOURCE_EXHAUSTED");
  }

  public static boolean isFailedPrecondition(Throwable t) {
    return t instanceof GoogleJsonResponseException
        && isFailedPrecondition((GoogleJsonResponseException) t);
  }

  public static boolean isFailedPrecondition(GoogleJsonResponseException e) {
    return matchesCodeAndError(e, BAD_REQUEST, GcpUtil::isFailedPrecondition);
  }

  public static boolean isFailedPrecondition(GoogleJsonError error) {
    return matchesStatus(error, "FAILED_PRECONDITION");
  }

  private static boolean matchesStatus(GoogleJsonError error, String status) {
    return status.equals(error.get("status"));
  }

  private static boolean matchesCodeAndError(GoogleJsonResponseException e, int code,
      Predicate<GoogleJsonError> errMatcher) {
    return e.getStatusCode() == code && Optional.ofNullable(e.getDetails())
        .map(errMatcher::test)
        .orElse(false);
  }
}
