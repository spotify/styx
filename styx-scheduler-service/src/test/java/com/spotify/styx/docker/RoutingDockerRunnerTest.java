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
  static final String MOCK_EXEC_ID = "mock-run-id-0";

  int createCounter = 0;
  Map<String, DockerRunner> createdRunners = Maps.newHashMap();
  Supplier<String> dockerId = mock(Supplier.class);

  DockerRunner dockerRunner;

  @Before
  public void setUp() throws Exception {
    dockerRunner = new RoutingDockerRunner(this::create, dockerId);
  }

  @Test
  public void testUsesCreatesRunnerOnStart() throws Exception {
    when(dockerId.get()).thenReturn("default");
    final String execId = dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey("default"));
    assertThat(execId, is(MOCK_EXEC_ID));
  }

  @Test
  public void testUsesDefaultRunnerOnCleanup() throws Exception {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.cleanup(MOCK_EXEC_ID);

    assertThat(createdRunners, hasKey("default"));
    verify(createdRunners.get("default")).cleanup(MOCK_EXEC_ID);
  }

  @Test
  public void testCreatesOnlyOneRunnerPerDockerId() throws Exception {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.cleanup(MOCK_EXEC_ID);
    dockerRunner.cleanup(MOCK_EXEC_ID);

    assertThat(createCounter, is(1));
    assertThat(createdRunners.keySet(), hasSize(1));
    assertThat(createdRunners, hasKey("default"));
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

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    Mockito.reset(dockerId);
    when(dockerId.get()).thenReturn("id-1", "id-2");

    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.close();

    assertThat(createCounter, is(2));
    assertThat(createdRunners.keySet(), hasSize(2));
    for (DockerRunner runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  private DockerRunner create(String id) {
    DockerRunner mock = mock(DockerRunner.class);
    createCounter++;

    try {
      when(mock.start(Mockito.any(), Mockito.any())).thenReturn(MOCK_EXEC_ID);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    createdRunners.put(id, mock);
    return mock;
  }
}
