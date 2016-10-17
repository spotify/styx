package com.spotify.styx.docker;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.testdata.TestData;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RoutingDockerRunnerTest {

  static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "param");
  static final DockerRunner.RunSpec RUN_SPEC = DockerRunner.RunSpec.create(
      "busybox", ImmutableList.of("foo", "bar"), Optional.empty());
  static final String MOCK_EXEC_ID = "mock-run-id-0";

  Map<String, DockerRunner> createdRunners = Maps.newHashMap();

  DockerRunner dockerRunner;

  @Before
  public void setUp() throws Exception {
    dockerRunner = DockerRunner.routing(this::create);
  }

  @Test // temporary test
  public void testUsesDefaultRunnerOnStart() throws Exception {
    final String execId = dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey("default"));
    assertThat(execId, is(MOCK_EXEC_ID));
  }

  @Test // temporary test
  public void testUsesDefaultRunnerOnCleanup() throws Exception {
    dockerRunner.cleanup(MOCK_EXEC_ID);

    assertThat(createdRunners, hasKey("default"));
    verify(createdRunners.get("default")).cleanup(MOCK_EXEC_ID);
  }

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.close();

    for (DockerRunner runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  private DockerRunner create(String id) {
    DockerRunner mock = mock(DockerRunner.class);

    try {
      when(mock.start(Mockito.any(), Mockito.any())).thenReturn(MOCK_EXEC_ID);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    createdRunners.put(id, mock);
    return mock;
  }
}
