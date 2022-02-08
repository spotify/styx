package com.spotify.styx.flyte.client;

import static com.spotify.styx.flyte.client.RpcHelper.getExecutionsListFilter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

public class RpcHelperTest {


  @Test
  public void testGtExecutionsListFilter() {
    final Instant someTime = LocalDateTime.of(2022, 2, 2, 12, 12, 05)
        .atZone(ZoneOffset.UTC)
        .toInstant();

    final String executionsListFilter = getExecutionsListFilter(
        someTime,
        Duration.of(24, ChronoUnit.HOURS),
        Duration.of(3, ChronoUnit.MINUTES));

    assertThat(executionsListFilter,
        equalTo("execution.phase in (RUNNING),execution.started_at>2022-02-01T12:12:05,execution.started_at<2022-02-02T12:09:05"));

  }
}
