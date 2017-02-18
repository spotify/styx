/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx;

import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.spotify.styx.Graph.Edge;
import com.spotify.styx.Graph.FlowEdge;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.ParameterUtil;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for making decisions on how to make further progress on states in
 * the {@link StateManager}. The general operation is such that each time the {@link #tick()}
 * method is called, the scheduler will inspect all the active states in {@link StateManager} and
 * determine if any of them should receive new events.
 *
 * <p>Currently the scheduler only cares about states that are in the {@link State#QUEUED} state or
 * have timed out according to the {@link TimeoutConfig}).
 *
 * <p>For all Queued states that are eligible for execution, the scheduler will determine which
 * ones to dequeue, while ensuring that the {@link Resource}s associated with each respective
 * {@link Workflow} is not exceeded. It will try to dequeue workflow instances fairly by globally
 * randomizing the dequeue order on each {@link #tick()}.
 */
public class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

  @VisibleForTesting
  static final String GLOBAL_RESOURCE_ID = "GLOBAL_STYX_CLUSTER";

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;
  private final WorkflowCache workflowCache;
  private final Storage storage;
  private final TriggerListener triggerListener;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager,
                   WorkflowCache workflowCache, Storage storage, TriggerListener triggerListener) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.workflowCache = Objects.requireNonNull(workflowCache);
    this.storage = Objects.requireNonNull(storage);
    this.triggerListener = Objects.requireNonNull(triggerListener);
  }

  void tick() {
    final Map<String, Resource> resources;
    final Optional<Long> globalConcurrency;
    try {
      resources = storage.resources().stream().collect(toMap(Resource::id, identity()));
      globalConcurrency = storage.globalConcurrency();
    } catch (IOException e) {
      LOG.warn("Failed to get resource limits", e);
      return;
    }

    final Map<WorkflowInstance, RunState> states = stateManager.activeStates();
    final Set<WorkflowInstance> excluded = new HashSet<>();
    final Map<WorkflowId, Set<String>> resourceRefs = new HashMap<>();
    final Map<String, Long> resourceUsage = new HashMap<>();

    // validate states for timeouts and unknown resource refs
    for (RunState runState : states.values()) {
      // send timeouts and exclude
      if (hasTimedOut(runState)) {
        sendTimeout(runState);
        excluded.add(runState.workflowInstance());
        continue;
      }

      WorkflowId workflowId = runState.workflowInstance().workflowId();
      Set<String> refs = workflowResources(workflowId);

      // add to resource usage if not queued
      if (runState.state() != State.QUEUED) {
        for (String ref : refs) {
          resourceUsage.compute(ref, (ignore, prev) -> (prev != null ? prev : 0L) + 1);
        }
      }

      // validate resource references if eligible for execution
      if (shouldExecute(runState)) {
        List<String> unknown = refs.stream()
            .filter(ref -> !resources.containsKey(ref))
            .collect(toList());

        if (!unknown.isEmpty()) {
          sendError(runState, format("Referenced resources not found: %s", unknown));
          excluded.add(runState.workflowInstance());
        } else {
          resourceRefs.putIfAbsent(workflowId, refs);
        }
      }
    }

    // sort out states eligible for scheduling
    final List<InstanceState> eligibleStates = states.entrySet().stream()
        .filter(entry -> !excluded.contains(entry.getKey()))
        .filter(entry -> shouldExecute(entry.getValue()))
        .map(entry -> InstanceState.create(entry.getKey(), entry.getValue()))
        .collect(toList());

    globalConcurrency.ifPresent(concurrency ->
        System.out.println(GLOBAL_RESOURCE_ID + " = " + concurrency));

    graph(resources, resourceUsage, resourceRefs, eligibleStates);
    triggerBackfills(eligibleStates);
  }

  private void graph(
      Map<String, Resource> resources,
      Map<String, Long> resourceUsage,
      Map<WorkflowId, Set<String>> resourceRefs,
      List<InstanceState> eligibleStates) {

    final int numInstances = eligibleStates.size();
    final int numResourceRefs = resourceRefs.values().stream().mapToInt(Set::size).sum();
    final int numNodes = numInstances + numResourceRefs + 2; // + source and sink

    final Graph graph = new Graph(numNodes);
    final AtomicInteger nodeCounter = new AtomicInteger(1); // 0 is the sink
    final Map<String, Set<Edge>> resourceEdges = new HashMap<>();
    final Map<String, Integer> instanceNodes = new HashMap<>();
    final Function<String, Integer> newNode = (obj) ->
        instanceNodes.computeIfAbsent(obj, (ignore) -> lt(numNodes, nodeCounter.getAndIncrement()));

    // set up resource reference paths for workflows, going to the sink
    // e.g. r2 -> r1 -> sink
    final Map<WorkflowId, Integer> resourcePathStarts = new HashMap<>();
    final String[] nodeResources = new String[numNodes];

    for (Entry<WorkflowId, Set<String>> wfResources : resourceRefs.entrySet()) {
      WorkflowId wfid = wfResources.getKey();
      int connect = graph.sink(); // todo: replace with global resource node before sink
      int rank = Graph.MAX_RANK - 1;

      for (String ref : wfResources.getValue()) {
        Resource resource = resources.get(ref);
        String nodeName = ref + "_lim:" + resource.concurrency();
        int node = newNode.apply(wfid.toKey() + nodeName);

        // remember the resource ref for the node
        nodeResources[node] = ref;

        graph.setName(node, nodeName);
        graph.setRank(node, rank);

        // create resource edge with remaining capacity
        long capacity = resource.concurrency() - resourceUsage.getOrDefault(ref, 0L);
        graph.createEdge(node, connect, (int) capacity);

        // alias edge with existing edges for same resource
        Set<Edge> edges = resourceEdges.computeIfAbsent(ref, (ignore) -> new HashSet<>());
        for (Edge edge : edges) {
          graph.createEdgeAlias(node, connect, edge.u(), edge.v());
        }
        edges.add(Edge.create(node, connect));

        connect = node;
        rank--;
      }

      // remember the last resource node for the workflow
      resourcePathStarts.put(wfid, connect);
    }

    // set up unit-capacity edges for eligible workflow instances from the source to the
    // start of the resource path, or directly to the sink if no resource path exists
    final InstanceState[] nodeInstances = new InstanceState[numNodes];
    final Map<WorkflowId, Set<InstanceState>> statesByWorkflow = eligibleStates.stream()
        .collect(groupingBy(
            instanceState -> instanceState.workflowInstance().workflowId(),
            toSet()));

    for (Entry<WorkflowId, Set<InstanceState>> wf : statesByWorkflow.entrySet()) {
      WorkflowId wfid = wf.getKey();
      int resourcePathNode = resourcePathStarts.getOrDefault(wfid, graph.sink());

      for (InstanceState instanceState : wf.getValue()) {
        int node = newNode.apply(instanceState.workflowInstance().toKey());

        // remember the instance state for the node
        nodeInstances[node] = instanceState;

        graph.setName(node, instanceState.workflowInstance().toKey());
        graph.setRank(node, 1);
        graph.createEdge(graph.source(), node, 1);
        graph.createEdge(node, resourcePathNode, 1);
      }
    }

    // find a maximum flow through the graph, which will tell us which instances to schedule
    // and which resources have been depleted
    graph.solve();

    // dequeue all instances with flow from the source
    Set<FlowEdge> flowFromSource = graph.flowingEdgesFrom(graph.source());
    for (FlowEdge flowEdge : flowFromSource) {
      InstanceState toTrigger = nodeInstances[flowEdge.edge().v()];
      sendDequeue(toTrigger);
    }

    // send resource depletion message to all instances with no flow from the source
    Set<FlowEdge> nonFlowFromSource = graph.nonFlowingEdgesFrom(graph.source());
    for (FlowEdge noFlowEdge : nonFlowFromSource) {
      InstanceState notTriggered = nodeInstances[noFlowEdge.edge().v()];
      List<String> depletedResources = new ArrayList<>();

      // todo: find better way to write this
      int resourcePathStart = resourcePathStarts.get(notTriggered.workflowInstance().workflowId());
      Optional<FlowEdge> resourceEdge = graph.edgesFrom(resourcePathStart)
          .stream()
          .findFirst();
      while (resourceEdge.isPresent()) {
        FlowEdge flowEdge = resourceEdge.get();

        if (flowEdge.flow() == flowEdge.capacity()) {
          depletedResources.add(nodeResources[flowEdge.edge().u()]);
        }

        resourceEdge = graph.edgesFrom(flowEdge.edge().v())
            .stream()
            .findFirst();
      }

      Message message = Message.info(
          String.format("Resource limit reached for: %s",
              depletedResources.stream()
                  .map(resources::get)
                  .collect(toList())));
      sendMessage(message, notTriggered);
    }

    graph.printDot();
  }

  private int lt(int max, int n) {
    if (n >= max) {
      throw new RuntimeException("Number (" + n + ") greater than or equal to " + max);
    }
    return n;
  }

  @VisibleForTesting
  private void triggerBackfills(Collection<InstanceState> activeStates) {
    final List<Backfill> backfills;
    try {
      backfills = storage.backfills().stream()
          // TODO: filter in datastore
          .filter(backfill -> !backfill.allTriggered())
          // TODO: filter in datastore
          .filter(backfill -> !backfill.halted())
          .collect(toList());
    } catch (IOException e) {
      LOG.warn("Failed to get backfills", e);
      return;
    }

    final Map<String, Long> backfillStates = activeStates.stream()
        .map(state -> state.runState().data().trigger())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(TriggerUtil::isBackfill)
        .collect(groupingBy(
            TriggerUtil::triggerId,
            HashMap::new,
            counting()));

    backfills.forEach(backfill -> {
      final BackfillBuilder builder = backfill.builder();

      final Optional<Workflow> workflowOpt = workflowCache.workflow(backfill.workflowId());

      if (!workflowOpt.isPresent()) {
        LOG.warn("workflow not found for backfill, skipping rest of triggers: {}", backfill);
        builder.halted(true);
        storeBackfill(builder.build());
        return;
      }

      final int needed = backfill.concurrency() - backfillStates.getOrDefault(backfill.id(), 0L).intValue();
      if (needed <= 0) {
        return;
      }

      final Workflow workflow = workflowOpt.get();

      final List<Instant> partitionsRemaining =
          ParameterUtil.rangeOfInstants(backfill.nextTrigger(), backfill.end(),
                                        workflow.schedule().partitioning());

      final List<Instant> partitionsNeeded =
          partitionsRemaining.stream().limit(needed).collect(toList());

      partitionsNeeded.forEach(
          partition -> {
            try {
              triggerListener.event(workflow, Trigger.backfill(backfill.id()), partition);
            } catch (AlreadyInitializedException e) {
              LOG.warn("tried to trigger backfill for already active state [{}]: {}",
                       partition, backfill);
            }
          });

      if (partitionsRemaining.size() > partitionsNeeded.size()) {
        builder.nextTrigger(partitionsRemaining.get(partitionsNeeded.size()));
      } else {
        builder.nextTrigger(backfill.end());
        builder.allTriggered(true);
      }

      storeBackfill(builder.build());
    });
  }

  private void storeBackfill(Backfill backfill) {
    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      LOG.warn("Failed to store updated backfill", e);
    }
  }

  private Set<String> workflowResources(WorkflowId workflowId) {
    final Optional<Workflow> workflowOpt = workflowCache.workflow(workflowId);

    // todo: ignore workflow if not present? instead of running it without resource limits
    if (!workflowOpt.isPresent()) {
      return emptySet();
    }

    // copy to set to deduplicate
    return Sets.newLinkedHashSet(workflowOpt.get().schedule().resources());
  }

  private boolean shouldExecute(RunState runState) {
    if (runState.state() != State.QUEUED) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plusMillis(runState.data().retryDelayMillis().orElse(0L));

    return !deadline.isAfter(now);
  }

  private void sendDequeue(InstanceState instanceState) {
    final WorkflowInstance workflowInstance = instanceState.workflowInstance();
    final RunState state = instanceState.runState();

    if (state.data().tries() == 0) {
      LOG.info("Triggering {}", workflowInstance.toKey());
    } else {
      LOG.info("{} executing retry #{}", workflowInstance.toKey(), state.data().tries());
    }
    stateManager.receiveIgnoreClosed(Event.dequeue(workflowInstance));
  }

  private boolean hasTimedOut(RunState runState) {
    if (runState.state().isTerminal()) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plus(ttls.ttlOf(runState.state()));

    return !deadline.isAfter(now);
  }

  private void sendTimeout(RunState runState) {
    LOG.info("Found stale state, issuing timeout for {}", runState.workflowInstance());
    stateManager.receiveIgnoreClosed(Event.timeout(runState.workflowInstance()));
  }

  private void sendMessage(Message message, InstanceState instance) {
    final List<Message> messages = instance.runState().data().messages();
    if (messages.size() == 0 || !message.equals(messages.get(messages.size() - 1))) {
      stateManager.receiveIgnoreClosed(Event.info(instance.workflowInstance(), message));
    }
  }

  private void sendError(RunState runState, String message) {
    stateManager.receiveIgnoreClosed(Event.runError(runState.workflowInstance(), message));
  }

  @AutoValue
  abstract static class InstanceState {
    abstract WorkflowInstance workflowInstance();
    abstract RunState runState();

    static InstanceState create(WorkflowInstance workflowInstance, RunState runState) {
      return new AutoValue_Scheduler_InstanceState(workflowInstance, runState);
    }
  }
}
