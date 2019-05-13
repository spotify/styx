/*-
 * -\-\-
 * Spotify End-to-End Integration Tests
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.e2e_tests;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DatastoreUtil {

  private static final Pattern RESERVED_KIND_PATTERN = Pattern.compile("__.*__");

  private DatastoreUtil() {
    throw new UnsupportedOperationException();
  }

  private static final Logger log = LoggerFactory.getLogger(DatastoreUtil.class);

  static void deleteDatastoreNamespace(Datastore datastore, String namespace) {
    var keys = new ArrayList<Key>();
    var query = datastore.run(KeyQuery.newKeyQueryBuilder().setNamespace(namespace).build());
    query.forEachRemaining(key -> {
      if (!RESERVED_KIND_PATTERN.matcher(key.getKind()).matches()) {
        keys.add(key);
      }
    });
    if (keys.isEmpty()) {
      return;
    }
    log.info("Deleting datastore entities in namespace {}: {}", namespace, keys.size());
    Lists.partition(keys, 500).forEach(keyBatch ->
        datastore.delete(keyBatch.toArray(Key[]::new)));
  }
}
