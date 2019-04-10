/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.docker;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.SecretListBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class Fabric8KubernetesClientTest {

  private static final String POD_NAME = "foo-pod";
  private static final String POD_UID = "bar-pod";
  private static final String POD_LIST_RESOURCE_VERSION = "baz-pods";

  private static final String SECRET_NAME = "foo-secret";
  private static final String SECRET_UID = "bar-secret";
  private static final String SECRET_LIST_RESOURCE_VERSION = "baz-secrets";

  @Mock KubernetesClient client;
  @Mock MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock PodResource<Pod, DoneablePod> namedPod;
  @Mock MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock Resource<Secret, DoneableSecret> namedSecret;
  @Mock Watcher<Pod> watcher;
  @Mock Watch watch;

  private Pod pod;
  private Pod createdPod;
  private Pod deletedPod;
  private PodList podList;
  private Secret secret;
  private Secret createdSecret;
  private SecretList secretList;

  private Fabric8KubernetesClient sut;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    sut = Fabric8KubernetesClient.of(client);

    pod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .endMetadata()
        .build();
    createdPod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withUid(POD_UID)
        .endMetadata()
        .build();
    deletedPod = new PodBuilder()
        .withNewMetadata()
        .withName(POD_NAME)
        .withUid(POD_UID)
        .withDeletionTimestamp(Instant.now().toString())
        .endMetadata()
        .build();
    podList = new PodListBuilder()
        .withNewMetadata().withResourceVersion(POD_LIST_RESOURCE_VERSION).endMetadata()
        .withItems(new ArrayList<>())
        .build();
    podList.getMetadata().setResourceVersion("baz");

    secret = new SecretBuilder()
        .withNewMetadata()
        .withName(SECRET_NAME)
        .endMetadata()
        .build();
    createdSecret = new SecretBuilder()
        .withNewMetadata()
        .withName(SECRET_NAME)
        .withUid(SECRET_UID)
        .endMetadata()
        .build();
    secretList = new SecretListBuilder()
        .withNewMetadata().withResourceVersion(SECRET_LIST_RESOURCE_VERSION).endMetadata()
        .withItems(new ArrayList<>())
        .build();
    secretList.getMetadata().setResourceVersion("baz");

    when(client.pods()).thenReturn(pods);
    when(pods.withName(any())).thenReturn(namedPod);
    when(pods.list()).thenReturn(podList);
    when(pods.watch(any())).thenReturn(watch);
    when(client.secrets()).thenReturn(secrets);
    when(secrets.withName(any())).thenReturn(namedSecret);
    when(secrets.list()).thenReturn(secretList);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(client);
    verifyNoMoreInteractions(pods);
    verifyNoMoreInteractions(namedPod);
    verifyNoMoreInteractions(secrets);
    verifyNoMoreInteractions(namedSecret);
    verifyNoMoreInteractions(watcher);
    verifyNoMoreInteractions(watch);
  }

  @Test
  public void shouldCreate() {
    assertThat(sut, is(not(nullValue())));
  }

  @Test
  public void shouldGetPod() {
    when(namedPod.get()).thenReturn(pod);
    var actual = sut.getPod(POD_NAME);
    assertThat(actual, is(Optional.of(pod)));
    verify(client).pods();
    verify(pods).withName(POD_NAME);
    verify(namedPod).get();
  }

  @Test
  public void shouldNotGetPod() {
    var actual = sut.getPod(POD_NAME);
    assertThat(actual, is(Optional.empty()));
    verify(client).pods();
    verify(pods).withName(POD_NAME);
    verify(namedPod).get();
  }

  @Test
  public void shouldListPods() {
    var actual = sut.listPods();
    assertThat(actual, is(podList));
    verify(client).pods();
    verify(pods).list();
  }

  @Test
  public void shouldCreatePod() {
    when(pods.create(pod)).thenReturn(createdPod);
    var actual = sut.createPod(pod);
    assertThat(actual, is(createdPod));
    verify(client).pods();
    verify(pods).create(pod);
  }

  @Parameters({ "true", "false" })
  @Test
  public void shouldDeletePod(boolean deleted) {
    when(namedPod.delete()).thenReturn(deleted);
    var actual = sut.deletePod(POD_NAME);
    assertThat(actual, is(deleted));
    verify(client).pods();
    verify(pods).withName(POD_NAME);
    verify(namedPod).delete();
  }

  @Test
  public void shouldWatchPods() throws IOException {
    when(pods.watch(watcher)).thenReturn(watch);
    var actual = sut.watchPods(watcher);
    verify(client).pods();
    verify(pods).watch(watcher);
    assertThat(actual, is(watch));
  }

  @Test
  public void shouldGetSecret() {
    when(namedSecret.get()).thenReturn(secret);
    var actual = sut.getSecret(SECRET_NAME);
    assertThat(actual, is(Optional.of(secret)));
    verify(client).secrets();
    verify(secrets).withName(SECRET_NAME);
    verify(namedSecret).get();
  }

  @Test
  public void shouldNotGetSecret() {
    var actual = sut.getSecret(SECRET_NAME);
    assertThat(actual, is(Optional.empty()));
    verify(client).secrets();
    verify(secrets).withName(SECRET_NAME);
    verify(namedSecret).get();
  }

  @Test
  public void shouldListSecrets() {
    var actual = sut.listSecrets();
    assertThat(actual, is(secretList));
    verify(client).secrets();
    verify(secrets).list();
  }

  @Test
  public void shouldCreateSecret() {
    when(secrets.create(secret)).thenReturn(createdSecret);
    var actual = sut.createSecret(secret);
    assertThat(actual, is(createdSecret));
    verify(client).secrets();
    verify(secrets).create(secret);
  }

  @Parameters({ "true", "false" })
  @Test
  public void shouldDeleteSecret(boolean deleted) {
    when(namedSecret.delete()).thenReturn(deleted);
    var actual = sut.deleteSecret(SECRET_NAME);
    assertThat(actual, is(deleted));
    verify(client).secrets();
    verify(secrets).withName(SECRET_NAME);
    verify(namedSecret).delete();
  }
}