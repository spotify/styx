package com.spotify.styx.api;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredActionAuthorizerTest {

  @Mock private Stats stats;
  @Mock private ActionAuthorizer actionAuthorizer = new ActionAuthorizer.Nop();
  @Mock private Workflow workflow;

  private Instant now = Instant.now();
  private Instant later = now.plusMillis(123);
  private Queue<Instant> times;
  private Time time = () -> times.poll();

  private ActionAuthorizer meteredActionAuthorizer;

  @Before
  public void setUp() {
    times = new LinkedList<>(Arrays.asList(now, later));
    meteredActionAuthorizer = new MeteredActionAuthorizer(actionAuthorizer, stats, time);
  }

  @Test
  public void authorizeCreateOrUpdateWorkflowAction() {
    meteredActionAuthorizer.authorizeCreateOrUpdateWorkflowAction(workflow);
    verify(stats).recordActionAuthorizationDuration("create-or-update", 123);
  }

  @Test
  public void authorizeDeleteWorkflowAction() {
    meteredActionAuthorizer.authorizeDeleteWorkflowAction(workflow);
    verify(stats).recordActionAuthorizationDuration("delete", 123);
  }

  @Test
  public void authorizePatchStateWorkflowAction() {
    meteredActionAuthorizer.authorizePatchStateWorkflowAction(workflow);
    verify(stats).recordActionAuthorizationDuration("patch", 123);
  }

  @Test
  public void updateMetricsWhenUnauthorized() {
    doThrow(ResponseException.class).when(actionAuthorizer).authorizePatchStateWorkflowAction(workflow);
    meteredActionAuthorizer.authorizePatchStateWorkflowAction(workflow);
    verify(stats).recordActionAuthorizationDuration("patch", 123);
  }
}
