#!/usr/bin/env bash

exec mvn -pl styx-service \
  -Dexec.classpathScope="test" \
  -Dexec.mainClass="com.spotify.styx.WorkflowFetch" \
  test-compile dependency:copy-dependencies \
  exec:java
