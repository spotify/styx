package com.spotify.styx;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Set;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

public class GraphTest {

  @Test
  public void generalExampleSolvedFlow() throws Exception {
    Graph graph = exampleGraph();
    int flow = graph.solve();

    assertThat(flow, equalTo(5));

    assertThat(graph, flow(0, 1, equalTo(2)));
    assertThat(graph, flow(0, 3, equalTo(3)));
    assertThat(graph, flow(1, 2, equalTo(2)));
    assertThat(graph, flow(2, 3, equalTo(1)));
    assertThat(graph, flow(2, 4, equalTo(1)));
    assertThat(graph, flow(3, 5, equalTo(4)));
    assertThat(graph, flow(4, 6, equalTo(1)));
    assertThat(graph, flow(5, 6, equalTo(4)));
  }

  @Test
  public void flowingEdges() throws Exception {
    Graph graph = exampleGraph();
    graph.solve();

    Set<Graph.FlowEdge> flowEdges = graph.flowingEdges();

    assertThat(flowEdges, contains(
        Graph.FlowEdge.create(Graph.Edge.create(0, 1), 2, 3),
        Graph.FlowEdge.create(Graph.Edge.create(0, 3), 3, 3),
        Graph.FlowEdge.create(Graph.Edge.create(1, 2), 2, 4),
        Graph.FlowEdge.create(Graph.Edge.create(2, 3), 1, 1),
        Graph.FlowEdge.create(Graph.Edge.create(2, 4), 1, 2),
        Graph.FlowEdge.create(Graph.Edge.create(3, 5), 4, 6),
        Graph.FlowEdge.create(Graph.Edge.create(4, 6), 1, 1),
        Graph.FlowEdge.create(Graph.Edge.create(5, 6), 4, 9)
    ));
  }

  @Test
  public void nonFlowingEdges() throws Exception {
    Graph graph = exampleGraph();
    graph.solve();

    Set<Graph.FlowEdge> nonFlowEdges = graph.nonFlowingEdges();

    assertThat(nonFlowEdges, contains(
        Graph.FlowEdge.create(Graph.Edge.create(2, 0), 0, 3),
        Graph.FlowEdge.create(Graph.Edge.create(3, 4), 0, 2),
        Graph.FlowEdge.create(Graph.Edge.create(4, 1), 0, 1)
    ));
  }

  @Test
  public void flowingEdgesPredicate() throws Exception {
    Graph graph = exampleGraph();
    graph.solve();

    Set<Graph.FlowEdge> flowEdges = graph.flowingEdgesFrom(2);

    assertThat(flowEdges, contains(
        Graph.FlowEdge.create(Graph.Edge.create(2, 3), 1, 1),
        Graph.FlowEdge.create(Graph.Edge.create(2, 4), 1, 2)
    ));
  }

  @Test
  public void nonFlowingEdgesPredicate() throws Exception {
    Graph graph = exampleGraph();
    graph.solve();

    Set<Graph.FlowEdge> nonFlowEdges = graph.nonFlowingEdgesFrom(2);

    assertThat(nonFlowEdges, contains(
        Graph.FlowEdge.create(Graph.Edge.create(2, 0), 0, 3)
    ));
  }

  /**
   * Example from https://en.wikipedia.org/wiki/Edmonds-Karp_algorithm#Example
   */
  private Graph exampleGraph() {
    Graph graph = new Graph(7);

    graph.createEdge(graph.source(), 1, 3);
    graph.createEdge(graph.source(), 3, 3);

    graph.createEdge(1, 2, 4);

    graph.createEdge(2, 0, 3);
    graph.createEdge(2, 3, 1);
    graph.createEdge(2, 4, 2);

    graph.createEdge(3, 4, 2);
    graph.createEdge(3, 5, 6);

    graph.createEdge(4, 1, 1);
    graph.createEdge(4, graph.sink(), 1);

    graph.createEdge(5, graph.sink(), 9);

    return graph;
  }

  /**
   * s - 1 --c(1)-- 4 - t
   *  `- 2 --c(1)-- 5 -'
   *  `- 3 --c(1)-- 6 -'
   *
   *  Edges 1-4, 2-5 and 3-6 are aliased and will share a common capacity of 2
   */
  @Test
  public void aliasedEdges() throws Exception {
    Graph graph = new Graph(8);

    graph.createEdge(graph.source(), 1, 1);
    graph.createEdge(graph.source(), 2, 1);
    graph.createEdge(graph.source(), 3, 1);

    graph.createEdge(1, 4, 2);
    graph.createEdge(2, 5, 2);
    graph.createEdge(3, 6, 2);

    graph.createEdge(4, graph.sink(), 1);
    graph.createEdge(5, graph.sink(), 1);
    graph.createEdge(6, graph.sink(), 1);

    graph.createEdgeAlias(1, 4, 2, 5);
    graph.createEdgeAlias(1, 4, 3, 6);
    graph.createEdgeAlias(2, 5, 3, 6);

    int flow = graph.solve();

    assertThat(flow, equalTo(2));
  }

  Matcher<Graph> flow(int u, int v, Matcher<Integer> flowMatcher) {
    String desc = String.format("has flow %d -> %d = ", u, v);

    return new FeatureMatcher<Graph, Integer>(flowMatcher, desc, "flow") {
      @Override
      protected Integer featureValueOf(Graph actual) {
        return actual.flow(u, v);
      }
    };
  }
}
