package com.spotify.styx.flyte.client;

import flyteidl.admin.ExecutionOuterClass;

public enum ExecutionMode {
  MANUAL,
  SCHEDULED,
  SYSTEM,
  RELAUNCH,
  CHILD_WORKFLOW;

  ExecutionOuterClass.ExecutionMetadata.ExecutionMode toProto() {
    switch (this) {
      case MANUAL:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL;
      case SCHEDULED:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
      case SYSTEM:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SYSTEM;
      case RELAUNCH:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.RELAUNCH;
      case CHILD_WORKFLOW:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.CHILD_WORKFLOW;
      default:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.UNRECOGNIZED;
    }
  }
}
