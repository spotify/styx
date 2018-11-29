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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.iam.v1.Iam;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Response;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AllAuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.NoAuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.WhitelistAuthorizationPolicy;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServiceAccountUsageAuthorizerTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("foo", "bar");
  private static final String PRINCIPAL_EMAIL = "user@corp.com";
  private static final String SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccount.com";
  private static final String SERVICE_ACCOUNT_PROJECT = "bar";
  private static final String SERVICE_ACCOUNT_USER_ROLE = "organizations/3141592/roles/StyxWorkflowServiceAccountUser";

  @Mock private AuthorizationPolicy authorizationPolicy;
  @Mock private GoogleCredential credential;
  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdToken.Payload idTokenPayload;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private CloudResourceManager crm;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;

  private final com.google.api.services.cloudresourcemanager.model.Binding projectBinding =
      new com.google.api.services.cloudresourcemanager.model.Binding();
  private final com.google.api.services.iam.v1.model.Binding saBinding =
      new com.google.api.services.iam.v1.model.Binding();;

  private ServiceAccountUsageAuthorizer sut;

  @Before
  public void setUp() throws IOException {
    projectBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    projectBinding.setMembers(new ArrayList<>());
    projectBinding.getMembers().add("user:someone@else.com");
    final com.google.api.services.cloudresourcemanager.model.Policy projectPolicy =
        new com.google.api.services.cloudresourcemanager.model.Policy();
    projectPolicy.setBindings(new ArrayList<>());
    projectPolicy.getBindings().add(projectBinding);
    saBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    saBinding.setMembers(new ArrayList<>());
    saBinding.getMembers().add("user:someone@else.com");
    final com.google.api.services.iam.v1.model.Policy saPolicy =
        new com.google.api.services.iam.v1.model.Policy();
    saPolicy.setBindings(new ArrayList<>());
    saPolicy.getBindings().add(saBinding);
    when(authorizationPolicy.shouldEnforceAuthorization(any(), any(), any())).thenReturn(true);
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(idTokenPayload.getEmail()).thenReturn(PRINCIPAL_EMAIL);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenReturn(projectPolicy);
    when((Object) iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)
        .execute()).thenReturn(saPolicy);
    sut = new ServiceAccountUsageAuthorizer.Impl(iam, crm, SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy);
  }

  @Test
  public void shouldDenyAccessIfPrincipalDoesNotHaveUserRole() {
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is("Missing role " + SERVICE_ACCOUNT_USER_ROLE
        + " on either the project " + SERVICE_ACCOUNT_PROJECT + " or the service account " + SERVICE_ACCOUNT));
  }

  @Test
  public void shouldAllowAccessIfNotEnforcingAuthorizationPolicy() {
    when(authorizationPolicy.shouldEnforceAuthorization(any(), any(), any())).thenReturn(false);
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnProject() {
    projectBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnSA() {
    saBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldFailIfNotAUserCreatedServiceAccount() {
    final String serviceAccount = "4711-compute@developer.gserviceaccount.com";
    final Response<?> error = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
    assertThat(error.status().code(), is(BAD_REQUEST.code()));
    assertThat(error.status().reasonPhrase(), is("Not a user created service account: " + serviceAccount));
  }

  @Test
  public void shouldFailIfProjectDoesNotExist() throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(BAD_REQUEST.code()));
    assertThat(response.status().reasonPhrase(), is("Project does not exist: " + SERVICE_ACCOUNT_PROJECT));
  }


  @Test
  public void shouldFailIfServiceAccountDoesNotExist() throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(BAD_REQUEST.code()));
    assertThat(response.status().reasonPhrase(), is("Service account does not exist: " + SERVICE_ACCOUNT));
  }

  @Test
  public void shouldFailIfProjectRequestFailsWithGoogleException() throws IOException {
    final Throwable cause = googleJsonResponseException(500);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldFailIfServiceAccountRequestFailsWithGoogleException() throws IOException {
    final Throwable cause = googleJsonResponseException(500);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldFailIfProjectRequestFailsWithIOException() throws IOException {
    final Throwable cause = new IOException();
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void shouldFailIfServiceAccountRequestFailsWithIOException() throws IOException {
    final Throwable cause = new IOException();
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    exception.expect(RuntimeException.class);
    exception.expectCause(is(cause));
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void testCreate() {
    final ServiceAccountUsageAuthorizer sut =
        ServiceAccountUsageAuthorizer.create(SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy, credential);
    assertThat(sut, is(notNullValue()));
  }

  @Test
  public void testNop() {
    final ServiceAccountUsageAuthorizer sut = ServiceAccountUsageAuthorizer.nop();
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
  }

  @Test
  public void noAuthorizationPolicyShouldNotEnforce() {
    final AuthorizationPolicy policy = new NoAuthorizationPolicy();
    assertThat(policy.shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken), is(false));
  }

  @Test
  public void allAuthorizationPolicyShouldEnforce() {
    final AuthorizationPolicy policy = new AllAuthorizationPolicy();
    assertThat(policy.shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken), is(true));
  }

  @Test
  public void whitelistAuthorizationPolicyShouldEnforceWhitelist() {
    final AuthorizationPolicy policy = new WhitelistAuthorizationPolicy(ImmutableList.of(WORKFLOW_ID));
    assertThat(policy.shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken), is(true));
    assertThat(policy.shouldEnforceAuthorization(WorkflowId.create("another", "workflow"), SERVICE_ACCOUNT, idToken),
        is(false));
  }

  private static Response<?> assertThrowsResponseException(Runnable r) {
    try {
      r.run();
      throw new AssertionError();
    } catch (ResponseException e) {
      return e.getResponse();
    }
  }

  private static GoogleJsonResponseException googleJsonResponseException(int code) {
    return new GoogleJsonResponseException(new Builder(code, "", new HttpHeaders()), new GoogleJsonError());
  }
}