package com.spotify.styx;

import static com.spotify.styx.util.GuardedRunnable.runGuarded;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class ScheduledExecutionUtil {

  private ScheduledExecutionUtil() {
    throw new UnsupportedOperationException();
  }

  public static void scheduleWithJitter(Runnable runnable, ScheduledExecutorService exec, Duration tickInterval) {
    final double jitter = ThreadLocalRandom.current().nextDouble(0.5, 1.5);
    final long delayMillis = (long) (jitter * tickInterval.toMillis());
    exec.schedule(() -> {
      runGuarded(runnable);
      scheduleWithJitter(runnable, exec, tickInterval);
    }, delayMillis, MILLISECONDS);
  }
}
