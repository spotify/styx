/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Utilities for propagating a {@link MDC} to tasks running on thread pools.
 */
public class MDCUtil {

  private static final Logger log = LoggerFactory.getLogger(MDCUtil.class);

  private MDCUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Runs tasks on the shared FJP, with MDC propagated from the calling thread.
   */
  public static Executor withMDC() {
    return runnable -> ForkJoinPool.commonPool().execute(withMDC(runnable));
  }

  /**
   * Runs tasks on the supplied executor, with MDC propagated from the calling thread.
   */
  public static Executor withMDC(Executor executor) {
    return runnable -> executor.execute(withMDC(runnable));
  }

  /**
   * Wrap a {@link Runnable} to use the {@link MDC} from the calling thread.
   */
  public static Runnable withMDC(Runnable r) {
    return new Runnable() {

      private Map<String, String> contextMap = safeCopyOfContextMap();

      @Override
      public void run() {
        safeSetContextMap(contextMap);
        try {
          r.run();
        } finally {
          safeResetContextMap();
        }
      }
    };
  }

  /**
   * Wrap a {@link Callable} to use the {@link MDC} from the calling thread.
   */
  public static <V> Callable<V> withMDC(Callable<V> r) {
    return new Callable<V>() {

      private Map<String, String> contextMap = safeCopyOfContextMap();

      @Override
      public V call() throws Exception {
        safeSetContextMap(contextMap);
        try {
          return r.call();
        } finally {
          safeResetContextMap();
        }
      }
    };
  }

  private static Map<String, String> safeCopyOfContextMap() {
    try {
      return MDC.getCopyOfContextMap();
    } catch (Exception e) {
      log.error("Failed to copy MDC", e);
      return null;
    }
  }

  private static void safeSetContextMap(Map<String, String> contextMap) {
    try {
      if (contextMap != null) {
        MDC.setContextMap(contextMap);
      } else {
        MDC.clear();
      }
    } catch (Exception e) {
      log.error("Failed to set MDC", e);
    }
  }

  private static void safeResetContextMap() {
    try {
      MDC.clear();
    } catch (Exception e) {
      log.error("Failed to reset MDC", e);
    }
  }
}
