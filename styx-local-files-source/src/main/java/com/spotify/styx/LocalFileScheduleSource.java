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

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;
import static java.util.Collections.emptySet;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.schedule.ScheduleSource;
import com.typesafe.config.Config;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ScheduleSource} that monitors a local directory for yaml files.
 *
 * <p>The contents of the files are parsed into a {@link YamlScheduleDefinition} and
 * {@link Workflow} objects.
 *
 * <p>todo
 * - handle if watch dir is deleted (warn, try to re-watch until dir appears again)
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

  private final Map<String, Set<Workflow>> workflows = Maps.newHashMap();
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
      LOG.error("Invalid path: {}", sourceDir, e);
      throw new RuntimeException("Can't load local file schedule source: invalid path", e);
    }

    final Stream<Path> list;
    try {
      list = Files.list(path);
    } catch (IOException e) {
      LOG.error("Failed to List: {}", sourceDir, e);
      throw new RuntimeException("Can't load local file schedule source: initial listing failed", e);
    }
    list.filter(this::isYamlFile).forEach(this::readFile);

    WatchService watcher;
    try {
      watcher = FileSystems.getDefault().newWatchService();
      path.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
    } catch (IOException e) {
      LOG.error("Could not watch: {}", path, e);
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
        final WatchKey key = watchService.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (key == null) {
          continue;
        }

        for (WatchEvent<?> event : key.pollEvents()) {
          final WatchEvent.Kind<?> kind = event.kind();

          if (kind == OVERFLOW) {
            continue;
          }

          final WatchEvent<Path> pathEvent = cast(event);
          final Path file = watchPath.resolve(pathEvent.context());
          if (!isYamlFile(file)) {
            continue;
          }

          final String componentId = componentId(file);
          LOG.debug("{} event for component {}, from file {}", kind, componentId, file);

          if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
            readFile(file);
          }

          if (kind == ENTRY_DELETE) {
            final Set<Workflow> deleted = workflows.getOrDefault(componentId, emptySet());
            deleted.forEach(removeListener);
            deleted.clear();
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

  private boolean isYamlFile(Path file) {
    final String fileName = file.getFileName().toString();
    return fileName.endsWith(".yaml") || fileName.endsWith(".yml");
  }

  private void readFile(Path file) {
    try {
      for (Workflow workflow : readWorkflows(file)) {
        workflows.computeIfAbsent(workflow.componentId(), (k) -> Sets.newHashSet())
            .add(workflow);
        changeListener.accept(workflow);
      }
    } catch (IOException e) {
      LOG.warn("Failed to read schedule definition {}", file, e);
    }
  }

  private List<Workflow> readWorkflows(Path path) throws IOException {
    final byte[] bytes = Files.readAllBytes(path);
    LOG.debug("Read yaml file \n{}", ByteString.of(bytes).utf8());

    final YamlScheduleDefinition definitions = Yaml.parseScheduleDefinition(bytes);
    LOG.debug("Parsed schedule definitions: {}", definitions);

    final String componentId = componentId(path);
    final URI componentUri = path.toUri();
    return definitions.schedules().stream()
        .map(schedule -> Workflow.create(componentId, componentUri, schedule))
        .collect(Collectors.toList());
  }

  private String componentId(Path path) {
    return path.getFileName().toString();
  }

  @SuppressWarnings("unchecked")
  private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>) event;
  }
}
