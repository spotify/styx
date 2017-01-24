/*-
 * -\-\-
 * Spotify Styx Local Files Schedule Source
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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.io.Resources;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.schedule.ScheduleSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LocalFileScheduleSourceTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private Closer closer = Closer.create();
  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  private Map<String, Workflow> workflows = Maps.newConcurrentMap();

  private Callable<Map<String, Workflow>> workflows() {
    return () -> workflows;
  }

  private Callable<Integer> workflowsSize() {
    return () -> workflows.size();
  }

  @After
  public void tearDown() throws Exception {
    closer.close();
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void shouldFailIfNoConfigurationKeySet() throws Exception {
    Config config = ConfigFactory.empty();
    ScheduleSource source = createSource(config);

    exception.expect(RuntimeException.class);
    exception.expectMessage("Can't load local file schedule source: not configured");

    source.start();
  }

  @Test
  public void shouldFailToStartIfDirDoesNotExist() throws Exception {
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", "/i/should/not/exist"
    ));
    ScheduleSource source = createSource(config);

    exception.expect(RuntimeException.class);
    exception.expectMessage("Can't load local file schedule source:");
    exception.expectCause(instanceOf(NoSuchFileException.class));

    source.start();
  }

  @Test
  public void shouldReadExistingFilesOnStartup() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file.yaml");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));

    Files.write(testPath, readResource("simple-def.yaml"));
    ScheduleSource source = createSource(config);

    source.start();

    await().until(workflows(), hasEntry("foo", simpleDef(testPath)));
  }

  @Test
  public void shouldReadYamlFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file.yaml");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));
    ScheduleSource source = createSource(config);
    source.start();

    Files.write(testPath, readResource("example-defs.yaml"));

    await().until(workflows(), hasEntry("foo", example1(testPath)));
    await().until(workflows(), hasEntry("bar", example2(testPath)));
  }

  @Test
  public void shouldTriggerChangeOnChangedFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file.yaml");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));

    ScheduleSource source = createSource(config);
    source.start();

    Files.write(testPath, readResource("simple-def.yaml"));
    await().until(workflows(), hasEntry("foo", simpleDef(testPath)));

    Files.write(testPath, readResource("different-def.yaml"));
    await().until(workflows(), hasEntry("foo", differentDef(testPath)));
  }

  @Test
  public void shouldTriggerRemoveOnDeletedFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file.yaml");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));

    ScheduleSource source = createSource(config);
    source.start();

    Files.write(testPath, readResource("simple-def.yaml"));
    await().until(workflows(), hasKey("foo"));

    Files.delete(testPath);
    await().until(workflowsSize(), is(0));
  }

  private ScheduleSource createSource(Config config) {
    return new LocalFileScheduleSource(
        config, closer, executor, this::changeListener, this::removeListener);
  }

  private void changeListener(Workflow workflow) {
    workflows.put(workflow.workflowId(), workflow);
  }

  private void removeListener(Workflow workflow) {
    workflows.remove(workflow.workflowId());
  }

  private byte[] readResource(String filename) throws IOException, URISyntaxException {
    URL resource = Resources.getResource(filename);
    return Files.readAllBytes(Paths.get(resource.toURI()));
  }

  // matching simple-def.yaml
  private Workflow simpleDef(Path testPath) {
    return Workflow.create(
        "test-file.yaml",
        testPath.toUri(),
        WorkflowConfiguration.create(
            "foo",
            Schedule.HOURS,
            empty(),
            empty(),
            Optional.of(emptyList()),
            empty(),
            empty(),
            emptyList()));
  }

  // matching different-def.yaml
  private Workflow differentDef(Path testPath) {
    return Workflow.create(
        "test-file.yaml",
        testPath.toUri(),
        WorkflowConfiguration.create(
            "foo",
            Schedule.DAYS,
            empty(),
            empty(),
            Optional.of(singletonList("foo")),
            empty(),
            empty(),
            emptyList()));
  }

  // matching first def from example-defs.yaml
  private Workflow example1(Path testPath) {
    return Workflow.create(
        "test-file.yaml",
        testPath.toUri(),
        WorkflowConfiguration.create(
            "foo",
            Schedule.HOURS,
            empty(),
            empty(),
            Optional.of(Arrays.asList("foo", "bar")),
            empty(),
            empty(),
            emptyList()));
  }

  // matching second def from example-defs.yaml
  private Workflow example2(Path testPath) {
    return Workflow.create(
        "test-file.yaml",
        testPath.toUri(),
        WorkflowConfiguration.create(
            "bar",
            Schedule.DAYS,
            empty(),
            empty(),
            Optional.of(Arrays.asList("baz", "bax")),
            empty(),
            empty(),
            emptyList()));
  }
}
