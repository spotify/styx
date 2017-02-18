package com.spotify.styx;

import static java.lang.Integer.min;
import static java.util.stream.Collectors.toList;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TODO: document.
 */
class Graph {

  public static final int MAX_RANK = Integer.MAX_VALUE;

  private static final Predicate<Edge> TRUE = edge -> true;
  private static final int SOURCE = 0;

  private final int nodes;
  private final int sink;

  private final String[] names;
  private final int[] ranks;

  private final int[][] capacity;
  private final int[][] residuals;
  private final int[][] flow;
  private final boolean[][] active;

  private final Map<Edge, Set<Edge>> aliases;

  // residual cap cf(u,v) = c(u,v) - f(u,v)
  // s = 0, t = nodes.length - 1

  // todo: resizing, adding new nodes

  Graph(int nodes) {
    this.nodes = nodes;
    this.sink = nodes - 1;

    int matrixSize = this.nodes;
    this.names = new String[matrixSize];
    this.ranks = new int[matrixSize];
    this.capacity = new int[matrixSize][matrixSize];
    this.residuals = new int[matrixSize][matrixSize];
    this.flow = new int[matrixSize][matrixSize];
    this.active = new boolean[matrixSize][matrixSize];

    this.aliases = new HashMap<>();

    this.names[SOURCE] = "source";
    this.names[this.sink] = "sink";
    this.ranks[this.sink] = MAX_RANK;
  }

  public int source() {
    return SOURCE;
  }

  public int sink() {
    return sink;
  }

  public void setName(int node, String name) {
    this.names[node] = name.replaceAll("[^a-zA-Z0-9_]", "_");
  }

  public void setRank(int node, int rank) {
    this.ranks[node] = rank;
  }

  public void createEdge(int u, int v, int capacity) {
    this.capacity[u][v] = capacity;
    this.residuals[u][v] = capacity;
    this.active[u][v] = true;
  }

  public void createEdgeAlias(int u, int v, int i, int j) {
    Preconditions.checkArgument(
        capacity[u][v] == capacity[i][j],
        "Aliased edges must have same capacity");

    Edge from = Edge.create(u, v);
    Edge to = Edge.create(i, j);

    Set<Edge> fromAliases = aliases.getOrDefault(from, new HashSet<>());
    fromAliases.add(to);
    aliases.put(from, fromAliases);

    Set<Edge> toAliases = aliases.getOrDefault(to, new HashSet<>());
    toAliases.add(from);
    aliases.put(to, toAliases);
  }

  /**
   * Solve for maximum flow through the graph and return the total flow.
   *
   * @return The total flow added during this solve call
   */
  public int solve() {
    int totalFlow = 0;

    while (true) {
      Path path = bfs();
      if (path.flowAmount == 0) {
        break;
      }

      totalFlow += path.flowAmount;
      for (int v = sink; v != SOURCE; v = path.parents[v]) {
        int u = path.parents[v];
        sendFlow(path.flowAmount, u, v);

        Edge edge = Edge.create(u, v);
        if (aliases.containsKey(edge)) {
          Set<Edge> aliases = this.aliases.get(edge);
          for (Edge alias : aliases) {
            capacity[alias.u()][alias.v()] -= path.flowAmount;
          }
        }
      }
    }

    return totalFlow;
  }

  public int flow(int u, int v) {
    return flow[u][v];
  }

  public int flow(Edge edge) {
    return flow(edge.u(), edge.v());
  }

  public int capacity(int u, int v) {
    return capacity[u][v];
  }

  public int capacity(Edge edge) {
    return capacity(edge.u(), edge.v());
  }

  public boolean active(int u, int v) {
    return active[u][v];
  }

  public boolean active(Edge edge) {
    return active(edge.u(), edge.v());
  }

  public Set<FlowEdge> flowingEdges() {
    return edges(edge -> flow(edge) > 0);
  }

  public Set<FlowEdge> flowingEdgesFrom(int from) {
    return edgesFrom(from, edge -> flow(edge) > 0);
  }

  public Set<FlowEdge> nonFlowingEdges() {
    return edges(edge -> flow(edge) == 0);
  }

  public Set<FlowEdge> nonFlowingEdgesFrom(int from) {
    return edgesFrom(from, edge -> flow(edge) == 0);
  }

  public Set<FlowEdge> edges(Predicate<Edge> predicate) {
    Set<FlowEdge> edges = new LinkedHashSet<>();

    for (int u = 0; u < flow.length; u++) {
      for (int v = 0; v < flow[u].length; v++) {
        Edge edge = Edge.create(u, v);
        if (active(edge) && predicate.test(edge)) {
          edges.add(FlowEdge.create(edge, flow(u, v), capacity(u, v)));
        }
      }
    }

    return edges;
  }

  public Set<FlowEdge> edgesFrom(int from) {
    return edgesFrom(from, TRUE);
  }

  public Set<FlowEdge> edgesFrom(int from, Predicate<Edge> predicate) {
    Set<FlowEdge> edges = new LinkedHashSet<>();

    for (int v = 0; v < flow[from].length; v++) {
      Edge edge = Edge.create(from, v);
      if (active(edge) && predicate.test(edge)) {
        edges.add(FlowEdge.create(edge, flow(from, v), capacity(from, v)));
      }
    }

    return edges;
  }

  private void sendFlow(int amount, int u, int v) {
    flow[u][v] += amount;
    flow[v][u] -= amount;
    residuals[u][v] -= amount;
    residuals[v][u] += amount;
  }

  private Path bfs() {
    int[] parents = new int[nodes];
    for (int i = 0; i < nodes; i++) {
      parents[i] = -1;
    }
    parents[SOURCE] = -2; // make sure source is not rediscovered

    int[] caps = new int[nodes]; // Capacity of found path to node
    caps[SOURCE] = Integer.MAX_VALUE;

    Queue<Integer> queue = new LinkedList<>();
    queue.add(SOURCE);

    while (!queue.isEmpty()) {
      int u = queue.remove();

      List<Integer> searchOrder = IntStream.range(0, nodes).boxed().collect(toList());
      Collections.shuffle(searchOrder);

      for (int v : searchOrder) {
        if (residuals[u][v] > 0 && parents[v] == -1) {
          parents[v] = u;
          caps[v] = min(caps[u], capacity[u][v] - flow[u][v]);
          if (v == sink) {
            return new Path(parents, caps[sink]);
          } else {
            queue.add(v);
          }
        }
      }
    }

    return new Path(parents, 0);
  }

  /**
   * digraph graphname {
   *   a -> b -> c [label="4"];
   *   b -> d [label="2"];
   * }
   */
  public void printDot() {
    System.out.println("digraph schedule {");
    System.out.println("  rankdir=LR;");
    System.out.println("  node [shape=box, fontsize=10, fontname=Arial, penwidth=0.5];");
    System.out.println("  edge [fontsize=8, fontname=Arial, penwidth=0.5, arrowsize=0.5]");
    System.out.println();

    for (int u = 0; u < nodes; u++) {
      for (int v = 0; v < nodes; v++) {
        if (active[u][v]) {
          String color =  (flow[u][v] > 0)
              ? "blue" : "gray";
          System.out.printf("  N%d_%s -> N%d_%s [label=\"%d/%d\", color=%s];\n",
              u, names[u], v, names[v], flow[u][v], capacity[u][v], color);

          Edge edge = Edge.create(u, v);
          if (aliases.containsKey(edge)) {
            for (Edge alias : aliases.get(edge)) {
              System.out.printf("  N%d_%s -> N%d_%s [style=dotted];\n",
                  u, names[u], alias.u(), names[alias.u()]);
            }
          }
        }
      }
    }

    Map<Integer, List<Integer>> rankNodes = new LinkedHashMap<>();
    for (int u = 0; u < nodes; u++) {
      rankNodes.computeIfAbsent(ranks[u], (ignore) -> new ArrayList<>()).add(u);
    }
    for (List<Integer> sameRankNodes : rankNodes.values()) {
      String sameRankNames = sameRankNodes.stream()
          .map(u -> "N" + u + "_" + names[u])
          .collect(Collectors.joining(" "));
      System.out.printf("  { rank=same; %s }\n", sameRankNames);
    }

    System.out.println("}");
  }

  @AutoValue
  public abstract static class Edge {
    public abstract int u();
    public abstract int v();

    public static Edge create(int u, int v) {
      return new AutoValue_Graph_Edge(u, v);
    }
  }

  @AutoValue
  public abstract static class FlowEdge {
    public abstract Edge edge();
    public abstract int flow();
    public abstract int capacity();

    public static FlowEdge create(Edge edge, int flow, int capacity) {
      return new AutoValue_Graph_FlowEdge(edge, flow, capacity);
    }
  }

  private static class Path {
    final int[] parents;
    final int flowAmount;

    Path(int[] parents, int flowAmount) {
      this.parents = parents;
      this.flowAmount = flowAmount;
    }
  }
}
