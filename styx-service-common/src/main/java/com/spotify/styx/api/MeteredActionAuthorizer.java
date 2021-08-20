package com.spotify.styx.api;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class MeteredActionAuthorizer implements ActionAuthorizer {

  private final ActionAuthorizer actionAuthorizer;
  private final Stats stats;
  private final Time time;

  public MeteredActionAuthorizer(ActionAuthorizer actionAuthorizer, Stats stats, Time time) {
    this.actionAuthorizer = Objects.requireNonNull(actionAuthorizer);
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
  }

  @Override
  public void authorizeCreateOrUpdateWorkflowAction(Workflow workflow) {
    authorizeWorkflowAction(workflow, "create-or-update");
  }

  @Override
  public void authorizeDeleteWorkflowAction(Workflow workflow) {
    authorizeWorkflowAction(workflow, "delete");
  }

  @Override
  public void authorizePatchStateWorkflowAction(Workflow workflow) {
    authorizeWorkflowAction(workflow, "patch");
  }

  private void authorizeWorkflowAction(Workflow workflow, String operation) {
    Instant t0 = time.get();
    try {
      actionAuthorizer.authorizePatchStateWorkflowAction(workflow);
    } finally {
      final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
      stats.recordActionAuthorizationDuration(operation, durationMillis);
    }
  }
}
