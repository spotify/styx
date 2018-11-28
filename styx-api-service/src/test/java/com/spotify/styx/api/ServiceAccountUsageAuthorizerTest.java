/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.api;

import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.styx.api.ServiceAccountUsageAuthorizer.SERVICE_ACCOUNT_USER_ROLE;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.iam.v1.Iam;
import com.spotify.apollo.Response;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServiceAccountUsageAuthorizerTest {

  private static final String PRINCIPAL_EMAIL = "user@corp.com";
  private static final String SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccount.com";
  private static final String SERVICE_ACCOUNT_PROJECT = "bar";

  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdToken.Payload idTokenPayload;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private CloudResourceManager crm;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;
  private com.google.api.services.cloudresourcemanager.model.Policy projectPolicy;
  private com.google.api.services.cloudresourcemanager.model.Binding projectBinding;
  private com.google.api.services.iam.v1.model.Policy saPolicy;
  private com.google.api.services.iam.v1.model.Binding saBinding;

  private ServiceAccountUsageAuthorizer sut;

  @Before
  public void setUp() throws IOException {
    projectBinding = new com.google.api.services.cloudresourcemanager.model.Binding();
    projectBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    projectBinding.setMembers(new ArrayList<>());
    projectBinding.getMembers().add("user:someone@else.com");
    projectPolicy = new com.google.api.services.cloudresourcemanager.model.Policy();
    projectPolicy.setBindings(new ArrayList<>());
    projectPolicy.getBindings().add(projectBinding);
    saBinding = new com.google.api.services.iam.v1.model.Binding();
    saBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    saBinding.setMembers(new ArrayList<>());
    saBinding.getMembers().add("user:someone@else.com");
    saPolicy = new com.google.api.services.iam.v1.model.Policy();
    saPolicy.setBindings(new ArrayList<>());
    saPolicy.getBindings().add(saBinding);
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(idTokenPayload.getEmail()).thenReturn(PRINCIPAL_EMAIL);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenReturn(projectPolicy);
    when((Object) iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)
        .execute()).thenReturn(saPolicy);
    sut = new ServiceAccountUsageAuthorizer(iam, crm);
  }

  @Test
  public void shouldDenyAccessIfPrincipalDoesNotHaveUserRole() {
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is("Missing role " + SERVICE_ACCOUNT_USER_ROLE
        + " on either the project " + SERVICE_ACCOUNT_PROJECT + " or the service account " + SERVICE_ACCOUNT));
  }

  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnProject() {
    projectBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    sut.authorizeServiceAccountUsage(SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnSA() {
    saBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    sut.authorizeServiceAccountUsage(SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldFailIfNotAUserCreatedServiceAccount() {
    final String serviceAccount = "4711-compute@developer.gserviceaccount.com";
    final Response<?> error = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(serviceAccount, idToken));
    assertThat(error.status().code(), is(BAD_REQUEST.code()));
    assertThat(error.status().reasonPhrase(), is("Not a user created service account: " + serviceAccount));
  }

  @Test
  public void shouldFailIfProjectDoesNotExist() {
    // TODO
  }

  @Test
  public void shouldFailIfServiceAccountDoesNotExist() {
    // TODO
  }

  private static Response<?> assertThrowsResponseException(Runnable r) {
    try {
      r.run();
      throw new AssertionError();
    } catch (ResponseException e) {
      return e.getResponse();
    }
  }
}