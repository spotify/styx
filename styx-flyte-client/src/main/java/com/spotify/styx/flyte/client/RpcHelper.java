package com.spotify.styx.flyte.client;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class RpcHelper {


  // Test this filter by printing it and using it on a flytectl command
  // example:  flytectl get executions -p PROJECT -d DOMAIN --filter.fieldSelector="execution.phase in (RUNNING),execution.started_at>2022-02-07T18:23:05,execution.started_at<2022-02-08T18:10:05" -o json
  public static String getExecutionsListFilter(Instant timeNow, Duration since, Duration to) {

    final String dateSince = timeNow.minus(since).minus(24, ChronoUnit.HOURS)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));


    String dateTo = timeNow.minus(to)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

    return String.format("execution.phase in (RUNNING),execution.started_at>%s,execution.started_at<%s", dateSince, dateTo);
  }
}
