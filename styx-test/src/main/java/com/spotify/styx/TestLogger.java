package com.spotify.styx;

import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogger extends RunListener {

  private static final Logger log = LoggerFactory.getLogger(TestLogger.class);

  @Override
  public void testStarted(Description description) {
    log.info("Test started: {}", description);
  }

  @Override
  public void testFinished(Description description) {
    log.info("Test finished: {}", description);
  }

  @Override
  public void testFailure(Failure failure) {
    log.info("Test failed: {}", failure, failure.getException());
  }

  @Override
  public void testAssumptionFailure(Failure failure) {
    log.info("Test assumption failed: {}", failure);
  }

  @Override
  public void testIgnored(Description description) {
    log.info("Test ignore: {}", description);
  }
}
