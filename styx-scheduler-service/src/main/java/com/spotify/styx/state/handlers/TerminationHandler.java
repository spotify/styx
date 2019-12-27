/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.state.handlers;

import static java.lang.Boolean.TRUE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.RetryUtil;
import com.spotify.styx.util.TriggerUtil;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.SimpleEvaluationContext;

/**
 * A {@link OutputHandler} that manages scheduling generation of {@link Event}s
 * as a response to the {@link RunState.State#TERMINATED} and {@link RunState.State#FAILED} states.
 */
public class TerminationHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TerminationHandler.class);

  // Retry cost is vaguely related to a max time period we're going to keep retrying a state.
  // See the different costs for failures and missing dependencies in RunState
  static final double MAX_RETRY_COST = 50.0;
  private static final int MISSING_DEPS_EXIT_CODE = 20;
  private static final int FAIL_FAST_EXIT_CODE = 50;
  public static final int MISSING_DEPS_RETRY_DELAY_MINUTES = 10;
  private static final long RETRY_CONDITION_EXPRESSION_CACHE_SIZE = 1000;

  private final RetryUtil retryUtil;

  private final Supplier<Map<WorkflowId, Workflow>> workflows;

  private final SpelExpressionParser expressionParser;
  private final Cache<String, Expression> retryConditionExpressionCache;

  public TerminationHandler(RetryUtil retryUtil, Supplier<Map<WorkflowId, Workflow>> workflows) {
    this.retryUtil = Objects.requireNonNull(retryUtil, "retryUtil");
    this.workflows = Objects.requireNonNull(workflows, "workflows");

    var config = new SpelParserConfiguration(SpelCompilerMode.IMMEDIATE, this.getClass().getClassLoader());
    expressionParser = new SpelExpressionParser(config);
    retryConditionExpressionCache = CacheBuilder.newBuilder()
        .maximumSize(RETRY_CONDITION_EXPRESSION_CACHE_SIZE)
        .build();
  }

  @Override
  public void transitionInto(RunState state, EventRouter eventRouter) {
    switch (state.state()) {
      case TERMINATED:
        if (state.data().lastExit().map(v -> v.equals(0)).orElse(false)) {
          eventRouter.receiveIgnoreClosed(Event.success(state.workflowInstance()), state.counter());
        } else {
          checkRetry(state, eventRouter);
        }
        break;

      case FAILED:
        checkRetry(state, eventRouter);
        break;

      default:
        // do nothing
    }
  }

  private void checkRetry(RunState state, EventRouter eventRouter) {
    final WorkflowInstance workflowInstance = state.workflowInstance();

    if (state.data().retryCost() < MAX_RETRY_COST) {
      final Optional<Integer> exitCode = state.data().lastExit();
      if (shouldFailFast(state, exitCode)) {
        eventRouter.receiveIgnoreClosed(Event.stop(workflowInstance), state.counter());
      } else {
        final long delayMillis;
        if (isMissingDependency(exitCode)) {
          delayMillis = Duration.ofMinutes(MISSING_DEPS_RETRY_DELAY_MINUTES).toMillis();
        } else {
          delayMillis = retryUtil.calculateDelay(state.data().consecutiveFailures()).toMillis();
        }
        eventRouter.receiveIgnoreClosed(Event.retryAfter(workflowInstance, delayMillis), state.counter());
      }
    } else {
      eventRouter.receiveIgnoreClosed(Event.stop(workflowInstance), state.counter());
    }
  }

  private static boolean isMissingDependency(Optional<Integer> exitCode) {
    return exitCode.map(c -> c == MISSING_DEPS_EXIT_CODE).orElse(false);
  }

  private boolean shouldFailFast(RunState state, Optional<Integer> exitCode) {
    if (exitCode.isPresent() && exitCode.orElseThrow() == FAIL_FAST_EXIT_CODE) {
      return true;
    }

    var workflow = workflows.get().get(state.workflowInstance().workflowId());
    if (workflow == null) {
      LOG.info("Workflow {} does not exist", state.workflowInstance().workflowId().toKey());
      return true;
    }

    return workflow.configuration().retryCondition()
        .map(s -> !retryConditionMet(state, exitCode, s))
        .orElse(false);
  }

  @VisibleForTesting
  boolean retryConditionMet(RunState state,
                            Optional<Integer> exitCode,
                            String retryCondition) {
    var context = SimpleEvaluationContext.forReadOnlyDataBinding().build();
    context.setVariable("exitCode", exitCode.orElse(null));
    context.setVariable("tries", state.data().tries());
    context.setVariable("triggerType", state.data().trigger().map(TriggerUtil::triggerType).orElse(null));
    context.setVariable("consecutiveFailures", state.data().consecutiveFailures());

    final Expression expression;
    try {
      expression = retryConditionExpressionCache.get(retryCondition,
          () -> expressionParser.parseRaw(retryCondition));
    } catch (ExecutionException | UncheckedExecutionException e) {
      LOG.debug("Failed to parse retry condition `{}`", retryCondition, e.getCause());
      return false;
    }

    try {
      return TRUE.equals(expression.getValue(context, Boolean.class));
    } catch (EvaluationException e) {
      LOG.debug("Failed to evaluate retry condition `{}`", retryCondition, e);
      return false;
    }
  }
}
