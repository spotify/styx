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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.github.rholder.retry.StopStrategies;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.model.MembersHasMember;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Response;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AllAuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.NoAuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.WhitelistAuthorizationPolicy;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.ArrayList;
import javaslang.control.Try;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class ServiceAccountUsageAuthorizerTest {

  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("foo", "bar");
  private static final String PRINCIPAL_EMAIL = "user@corp.com";
  private static final String PROJECT_ADMINS_GROUP_EMAIL = "project-admins@corp.com";
  private static final String SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL = "service-account-admins@corp.com";
  private static final String SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccount.com";
  private static final String MANAGED_SERVICE_ACCOUNT = "4711-compute@developer.gserviceaccount.com";
  private static final String SERVICE_ACCOUNT_PROJECT = "bar";
  private static final String SERVICE_ACCOUNT_USER_ROLE = "organizations/3141592/roles/StyxWorkflowServiceAccountUser";
  private static final int RETRY_ATTEMPTS = 3;

  @Mock private AuthorizationPolicy authorizationPolicy;
  @Mock private PrivateKey privateKey;
  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdToken.Payload idTokenPayload;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private CloudResourceManager crm;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Directory directory;

  private final com.google.api.services.cloudresourcemanager.model.Binding projectBinding =
      new com.google.api.services.cloudresourcemanager.model.Binding();
  private final com.google.api.services.iam.v1.model.Binding saBinding =
      new com.google.api.services.iam.v1.model.Binding();

  private ServiceAccountUsageAuthorizer sut;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    projectBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    projectBinding.setMembers(new ArrayList<>());
    projectBinding.getMembers().add("user:someone@else.com");
    projectBinding.getMembers().add("group:" + PROJECT_ADMINS_GROUP_EMAIL);
    final com.google.api.services.cloudresourcemanager.model.Policy projectPolicy =
        new com.google.api.services.cloudresourcemanager.model.Policy();
    projectPolicy.setBindings(new ArrayList<>());
    projectPolicy.getBindings().add(projectBinding);
    saBinding.setRole(SERVICE_ACCOUNT_USER_ROLE);
    saBinding.setMembers(new ArrayList<>());
    saBinding.getMembers().add("user:someone@else.com");
    saBinding.getMembers().add("group:" + SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL);
    final com.google.api.services.iam.v1.model.Policy saPolicy =
        new com.google.api.services.iam.v1.model.Policy();
    saPolicy.setBindings(new ArrayList<>());
    saPolicy.getBindings().add(saBinding);
    when(authorizationPolicy.shouldEnforceAuthorization(any(), any(), any())).thenReturn(true);
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(idTokenPayload.getEmail()).thenReturn(PRINCIPAL_EMAIL);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenReturn(projectPolicy);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenReturn(saPolicy);
    when((Object) directory.members().hasMember(any(), any()).execute())
        .thenReturn(new MembersHasMember().setIsMember(false));
    when((Object) iam.projects().serviceAccounts().get(any()).execute())
        .thenReturn(new ServiceAccount()
            .setEmail(MANAGED_SERVICE_ACCOUNT)
            .setProjectId(SERVICE_ACCOUNT_PROJECT));
    sut = new ServiceAccountUsageAuthorizer.Impl(iam, crm, directory, SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy,
        StopStrategies.stopAfterAttempt(RETRY_ATTEMPTS));
  }

  @Test
  public void shouldDenyAccessIfNoPolicyBinding() throws IOException {
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenReturn(
        new com.google.api.services.cloudresourcemanager.model.Policy());
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenReturn(
        new com.google.api.services.iam.v1.model.Policy());

    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(SERVICE_ACCOUNT)));

    verify(iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)).execute();
    verify(crm.projects().getIamPolicy(eq(SERVICE_ACCOUNT_PROJECT), any())).execute();

    verify(directory.members(), never()).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    verify(directory.members(), never()).hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
  }

  @Test
  public void shouldDenyAccessIfPrincipalDoesNotHaveUserRole() throws IOException {
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(SERVICE_ACCOUNT)));

    verify(iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)).execute();
    verify(crm.projects().getIamPolicy(eq(SERVICE_ACCOUNT_PROJECT), any())).execute();
    verify(directory.members()).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    verify(directory.members()).hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
  }

  @Test
  public void shouldCacheAccessDenial() {
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(SERVICE_ACCOUNT)));

    reset(iam);
    reset(crm);
    reset(directory);

    for (int i = 0; i < 3; i++) {
      final Response<?> repeated = assertThrowsResponseException(() ->
          sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
      assertThat(repeated, is(response));
    }

    verifyZeroInteractions(iam);
    verifyZeroInteractions(crm);
    verifyZeroInteractions(directory);
  }

  @Test
  public void shouldLookupManagedServiceAccountAndCacheResult() throws IOException {
    for (int i = 0; i < 3; i++) {
      Try.run(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, MANAGED_SERVICE_ACCOUNT, idToken));
    }
    verify(iam.projects().serviceAccounts(), times(1)).get("projects/-/serviceAccounts/" + MANAGED_SERVICE_ACCOUNT);
  }

  @Test
  public void shouldFailIfManagedServiceAccountLookupFailsWithGoogleException() throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) iam.projects().serviceAccounts().get(any()).execute()).thenThrow(cause);
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, MANAGED_SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(BAD_REQUEST.code()));
    assertThat(response.status().reasonPhrase(), is("Service account does not exist: " + MANAGED_SERVICE_ACCOUNT));
  }

  @Test
  public void shouldFailIfManagedServiceAccountLookupFailsWithIOException() throws IOException {
    final Throwable cause = new IOException();
    when((Object) iam.projects().serviceAccounts().get(any()).execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, MANAGED_SERVICE_ACCOUNT, idToken)).getCause()), is(cause));
    verify(iam.projects().serviceAccounts().get(any()), atLeast(RETRY_ATTEMPTS)).execute();
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAllowAccessOnFailureIfNotEnforcingAuthorizationPolicy(String serviceAccount) throws IOException {
    final Throwable cause = new AssertionError();
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    when(authorizationPolicy.shouldEnforceAuthorization(any(), any(), any())).thenReturn(false);
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken);
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAllowAccessIfNotEnforcingAuthorizationPolicy(String serviceAccount) {
    when(authorizationPolicy.shouldEnforceAuthorization(any(), any(), any())).thenReturn(false);
    sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken);
    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, serviceAccount, idToken);
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnProjectDirectly(String serviceAccount) {
    projectBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnProjectViaGroup(String serviceAccount) throws IOException {
    when((Object) directory.members().hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL).execute())
        .thenReturn(new MembersHasMember().setIsMember(true));
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnServiceAccountDirectly(String serviceAccount) {
    saBinding.getMembers().add("user:" + PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnServiceAccountViaGroup(String serviceAccount) throws IOException {
    when((Object) directory.members().hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL).execute())
        .thenReturn(new MembersHasMember().setIsMember(true));
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfProjectDoesNotExist(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
    assertThat(response.status().code(), is(BAD_REQUEST.code()));
    assertThat(response.status().reasonPhrase(), is("Project does not exist: " + SERVICE_ACCOUNT_PROJECT));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfServiceAccountIamPolicyDoesNotExist(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
    assertThat(response.status().code(), is(BAD_REQUEST.code()));
    assertThat(response.status().reasonPhrase(), is("Service account does not exist: " + serviceAccount));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfProjectRequestFailsWithGoogleException(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(500);
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(crm.projects().getIamPolicy(any(), any()), atLeast(RETRY_ATTEMPTS)).execute();
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfServiceAccountIamPolicyRequestFailsWithGoogleException(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(500);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(iam.projects().serviceAccounts().getIamPolicy(any()), atLeast(RETRY_ATTEMPTS)).execute();
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfProjectRequestFailsWithIOException(String serviceAccount) throws IOException {
    final Throwable cause = new IOException();
    when((Object) crm.projects().getIamPolicy(any(), any()).execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(crm.projects().getIamPolicy(any(), any()), atLeast(RETRY_ATTEMPTS)).execute();
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfServiceAccountRequestFailsWithIOException(String serviceAccount) throws IOException {
    final Throwable cause = new IOException();
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(iam.projects().serviceAccounts().getIamPolicy(any()), atLeast(RETRY_ATTEMPTS)).execute();
  }

  @Test
  public void testCreate() {
    final GoogleCredential credential = new GoogleCredential.Builder()
        .setServiceAccountPrivateKey(privateKey)
        .setServiceAccountId("styx@bar.iam.gserviceaccount.com")
        .build();
    final ServiceAccountUsageAuthorizer sut = ServiceAccountUsageAuthorizer.create(
        SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy, credential, "gsuite-user@example.com", "foo");
    assertThat(sut, is(notNullValue()));
  }

  @Test
  public void testNop() {
    final ServiceAccountUsageAuthorizer sut = ServiceAccountUsageAuthorizer.NOP;
    assertThat(Try.run(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken)).isSuccess(),
        is(true));
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

  private void assertCachedSuccess(Runnable r) {
    r.run();

    reset(iam);
    reset(crm);
    reset(directory);

    for (int i = 0; i < 3; i++) {
      r.run();
    }

    verifyZeroInteractions(iam);
    verifyZeroInteractions(crm);
    verifyZeroInteractions(directory);
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

  private static String deniedMessage(String serviceAccount) {
    return "The user " + PRINCIPAL_EMAIL + " must have the role " +
           SERVICE_ACCOUNT_USER_ROLE + " on the project " + SERVICE_ACCOUNT_PROJECT + " or the service account " +
           serviceAccount + ", either through a group membership (recommended) or directly";
  }
}
