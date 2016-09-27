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

import com.google.common.io.Closer;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.schedule.ScheduleSource;
import com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import okio.ByteString;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 *
 */
class LocalFileScheduleSource implements ScheduleSource {

  private static final String LOCAL_DIR_CONFIG_KEY = "styx.source.local.dir";
  private static final long POLL_TIMEOUT_MILLIS = 100;

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileScheduleSource.class);

  private final Config config;
  private final Closer closer;
  private final ScheduledExecutorService executor;
  private final Consumer<Workflow> changeListener;
  private final Consumer<Workflow> removeListener;

  private volatile boolean running;

  LocalFileScheduleSource(
      Config config,
      Closer closer,
      ScheduledExecutorService executor,
      Consumer<Workflow> changeListener,
      Consumer<Workflow> removeListener) {
    this.config = Objects.requireNonNull(config);
    this.closer = Objects.requireNonNull(closer);
    this.executor = Objects.requireNonNull(executor);
    this.changeListener = Objects.requireNonNull(changeListener);
    this.removeListener = Objects.requireNonNull(removeListener);
  }

  @Override
  public void start() {
    if (!config.hasPath(LOCAL_DIR_CONFIG_KEY)) {
      LOG.error("Configuration key '{}' not set", LOCAL_DIR_CONFIG_KEY);
      throw new RuntimeException("Can't load local file schedule source: not configured");
    }

    final String sourceDir = config.getString(LOCAL_DIR_CONFIG_KEY);
    final Path path;
    try {
      path = Paths.get(sourceDir);
    } catch (InvalidPathException e) {
      LOG.error("Invalid path: {}", sourceDir);
      throw new RuntimeException("Can't load local file schedule source: invalid path");
    }

    WatchService watcher;
    try {
      watcher = FileSystems.getDefault().newWatchService();
      path.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
    } catch (IOException e) {
      LOG.error("Could not watch: {}", path);
      throw new RuntimeException("Can't load local file schedule source", e);
    }

    running = true;
    closer.register(() -> running = false);
    executor.submit(() -> poll(path, watcher));
  }

  private void poll(Path watchPath, WatchService watchService) {
    LOG.info("Watching {} for schedule definitions", watchPath);

    try {
      while (running) {
        WatchKey key = watchService.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (key == null) {
          continue;
        }

        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();

          if (kind == OVERFLOW) {
            continue;
          }

          WatchEvent<Path> pathEvent = cast(event);
          Path filename = pathEvent.context();
          Path file = watchPath.resolve(filename);

          System.out.println("kind = " + kind);
          System.out.println("event.count() = " + event.count());
          System.out.println("filename = " + filename);
          System.out.println("file = " + file);

          if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
            try {
              readWorkflows(file).forEach(changeListener);
            } catch (IOException e) {
              LOG.warn("Failed to read schedule definition {}", filename, e);
            }
          }

          if (kind == ENTRY_DELETE) {
            removeListener.accept(null);
          }
        }

        // Reset the key -- this step is critical if you want to
        // receive further watch events. If the key is no longer valid,
        // the directory is inaccessible so exit the loop.
        boolean valid = key.reset();
        if (!valid) {
          break;
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("interrupted", e);
    }

    LOG.info("Stopped watching {}", watchPath);
  }

  private List<Workflow> readWorkflows(Path path) throws IOException {
    final byte[] bytes = Files.readAllBytes(path);

    System.out.println("bytes = " + ByteString.of(bytes).utf8());
    final YamlScheduleDefinition definitions = Yaml.parseScheduleDefinition(bytes);
    System.out.println("definitions = " + definitions);

    final String componentId = path.getFileName().toString();
    final URI componentUri = path.toUri();
    return definitions.schedules().stream()
        .map(schedule -> Workflow.create(componentId, componentUri, schedule))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>) event;
  }
}
