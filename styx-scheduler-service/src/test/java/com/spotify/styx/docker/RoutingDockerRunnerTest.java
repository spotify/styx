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
import java.util.function.Supplier;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
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
  static final String DEFAULT_ID = "default";
  static final String MOCK_EXEC_ID = "mock-run-id-0";

  Map<String, DockerRunner> createdRunners = Maps.newHashMap();
  Supplier<String> dockerId = mock(Supplier.class);

  DockerRunner dockerRunner;

  @Before
  public void setUp() throws Exception {
    when(dockerId.get()).thenReturn(DEFAULT_ID);
    dockerRunner = new RoutingDockerRunner(this::create, dockerId);
  }

  @Test
  public void testUsesDefaultRunnerOnStart() throws Exception {
    final String execId = dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey(DEFAULT_ID));
    assertThat(execId, is(MOCK_EXEC_ID));
  }

  @Test
  public void testUsesDefaultRunnerOnCleanup() throws Exception {
    dockerRunner.cleanup(MOCK_EXEC_ID);

    assertThat(createdRunners, hasKey(DEFAULT_ID));
    verify(createdRunners.get(DEFAULT_ID)).cleanup(MOCK_EXEC_ID);
  }

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    Mockito.reset(dockerId);
    when(dockerId.get()).thenReturn("id-1", "id-2");

    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.close();

    assertThat(createdRunners.keySet(), hasSize(2));
    for (DockerRunner runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  @Test
  public void testSwitchesDockerRunner() throws Exception {
    Mockito.reset(dockerId);
    when(dockerId.get()).thenReturn("id-1", "id-2");

    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey("id-1"));
    assertThat(createdRunners, hasKey("id-2"));
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
