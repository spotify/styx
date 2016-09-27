/*
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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.schedule.ScheduleSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.Files.write;
import static okio.ByteString.encodeUtf8;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.fail;

/**
 * todo
 *
 * - handle create/modify and delete events
 * - handle if watch dir is deleted (warn, try to re-watch until dir appears again)
 */
public class LocalFileScheduleSourceTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private Closer closer = Closer.create();
  private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  private CountDownLatch changeEvents = new CountDownLatch(0);
  private CountDownLatch removeEvents = new CountDownLatch(0);

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
    exception.expectMessage("Can't load local file schedule source");
    exception.expectCause(instanceOf(NoSuchFileException.class));

    source.start();
  }

  @Test
  public void shouldTriggerChangeOnNewFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));
    ScheduleSource source = createSource(config);
    source.start();

    expectChangeEvents(1);
    Files.createFile(testPath).toFile();
    awaitEvents(changeEvents);
  }

  @Test
  public void shouldTriggerChangeOnChangedFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));

    ScheduleSource source = createSource(config);
    source.start();

    expectChangeEvents(1);
    File newFile = Files.createFile(testPath).toFile();
    awaitEvents(changeEvents);

    expectChangeEvents(1);
    write(encodeUtf8("test").toByteArray(), newFile);
    awaitEvents(changeEvents);
  }

  @Test
  public void shouldTriggerRemoveOnDeletedFiles() throws Exception {
    Path tmp = Files.createTempDirectory("styx");
    Path testPath = tmp.resolve("test-file");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.source.local.dir", tmp.toString()
    ));

    Files.createFile(testPath).toFile();

    ScheduleSource source = createSource(config);
    source.start();

    expectRemoveEvents(1);
    Files.delete(testPath);
    awaitEvents(removeEvents);
  }

  private ScheduleSource createSource(Config config) {
    return new LocalFileScheduleSource(
        config, closer, executor, this::changeListener, this::removeListener);
  }

  private void expectChangeEvents(int count) {
    changeEvents = new CountDownLatch(count);
  }

  private void expectRemoveEvents(int count) {
    removeEvents = new CountDownLatch(count);
  }

  private void awaitEvents(CountDownLatch latch) throws InterruptedException {
    if (!latch.await(30, TimeUnit.SECONDS)) {
      fail("Timed out while waiting for change events to happen");
    }
  }

  private void changeListener(Workflow workflow) {
    changeEvents.countDown();
  }

  private void removeListener(Workflow workflow) {
    removeEvents.countDown();
  }
}
