#!/usr/bin/env bash

# Run the ProductionStateRestore which validates that Styx can start and restore all state events
# currently stored in the `active_states` and `styx_events` tables

exec mvn -pl styx-scheduler-service \
  -Dexec.classpathScope="test" \
  -Dexec.mainClass="com.spotify.styx.ProductionStateRestore" \
  test-compile dependency:copy-dependencies \
  exec:java
