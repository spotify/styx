package com.spotify.styx.model;

import io.norberg.automatter.AutoMatter;

@AutoMatter
public interface TriggerResponse extends TriggerRequest {
  String triggerId();



  static TriggerResponseBuilder builder() {
    return new TriggerResponseBuilder();
  }

  static TriggerResponse of(WorkflowId workflowId, String parameter, String triggerId) {
    return builder()
        .workflowId(workflowId)
        .parameter(parameter)
        .triggerId(triggerId)
        .build();
  }

  static TriggerResponse of(WorkflowId workflowId, String parameter,
                           TriggerParameters triggerParameters, String triggerId) {
    return builder()
        .workflowId(workflowId)
        .parameter(parameter)
        .triggerParameters(triggerParameters)
        .triggerId(triggerId)
        .build();
  }
}
