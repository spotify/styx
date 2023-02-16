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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.GetPolicyOptions;
import com.google.api.services.directory.Directory;
import com.google.api.services.directory.model.MembersHasMember;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AllAuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AuthorizationPolicy;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.WhitelistAuthorizationPolicy;
import com.spotify.styx.model.WorkflowId;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
  private static final String MESSAGE = "See more at https://example.com/docs/styx/authorization.";
  private static final String GSUITE_USER_EMAIL = "gsuite-user@example.com";
  private static final String ADMIN_EMAIL = "admin@corp.com";
  private static final String ADMIN_AGENT_EMAIL = "admin-agent@corp.gserviceaccount.com";
  private static final String STYX_ADMINS_GROUP_EMAIL = "styx-admins@corp.com";
  private static final String AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG =
      "styx.authorization.service-account-user-role";
  private static final String AUTHORIZATION_REQUIRE_ALL_CONFIG = "styx.authorization.require.all";
  private static final String AUTHORIZATION_REQUIRE_WORKFLOWS = "styx.authorization.require.workflows";
  private static final String AUTHORIZATION_GSUITE_USER_CONFIG = "styx.authorization.gsuite-user";
  private static final String AUTHORIZATION_MESSAGE_CONFIG = "styx.authorization.message";
  private static final String AUTHORIZATION_ADMINISTRATORS_CONFIG = "styx.authorization.administrators";
  private static final String AUTHORIZATION_BLACKLIST_CONFIG = "styx.authorization.blacklist";

  private static final List<String> ADMINISTRATORS = List.of(
      "user:" + ADMIN_EMAIL,
      "group:" + STYX_ADMINS_GROUP_EMAIL,
      "serviceAccount:" + ADMIN_AGENT_EMAIL);

  private static final String BLACKLISTED_USER_EMAIL = "blacklisted-user@corp.com";
  private static final String BLACKLISTED_GROUP_EMAIL = "blacklisted-group@corp.com";
  private static final String BLACKLISTED_SA_EMAIL = "blacklisted-sa@bar.iam.gserviceaccount.com";

  private static final List<String> BLACKLIST = List.of(
      "user:" + BLACKLISTED_USER_EMAIL,
      "group:" + BLACKLISTED_GROUP_EMAIL,
      "serviceAccount:" + BLACKLISTED_SA_EMAIL);

  private static final GetIamPolicyRequest GET_IAM_POLICY_REQUEST =
      new GetIamPolicyRequest().setOptions(new GetPolicyOptions().setRequestedPolicyVersion(3));

  @Mock private AuthorizationPolicy authorizationPolicy;
  @Mock private PrivateKey privateKey;
  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdToken.Payload idTokenPayload;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private CloudResourceManager crm;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;
  @Mock private CloudResourceManager.Projects.GetIamPolicy getIamPolicy;
  @Mock private Directory directory;
  @Mock private Directory.Members members;
  @Mock private Directory.Members.HasMember isMember;
  @Mock private Directory.Members.HasMember isNotMember;
  @Mock private Environment environment;

  private GoogleCredentials credential;

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
    when((Object) getIamPolicy.execute()).thenReturn(projectPolicy);
    when((Object) crm.projects().getIamPolicy(any(), eq(GET_IAM_POLICY_REQUEST))).thenReturn(getIamPolicy);
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenReturn(saPolicy);
    doReturn(members).when(directory).members();
    doReturn(isNotMember).when(members).hasMember(any(), any());
    doReturn(new MembersHasMember().setIsMember(true)).when(isMember).execute();
    doReturn(new MembersHasMember().setIsMember(false)).when(isNotMember).execute();
    when((Object) iam.projects().serviceAccounts().get(any()).execute())
        .thenReturn(new ServiceAccount()
            .setEmail(MANAGED_SERVICE_ACCOUNT)
            .setProjectId(SERVICE_ACCOUNT_PROJECT));
    credential = ServiceAccountCredentials.newBuilder()
        .setPrivateKey(privateKey)
        .setClientEmail("styx@bar.iam.gserviceaccount.com")
        .build();
    sut = new ServiceAccountUsageAuthorizer.Impl(iam, crm, directory, SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy,
        WaitStrategies.noWait(), StopStrategies.stopAfterAttempt(RETRY_ATTEMPTS), MESSAGE, ADMINISTRATORS, BLACKLIST);
  }

  @Test
  public void shouldDenyAccessIfPrincipalIsBlacklisted() {
    reset(iam);
    reset(crm);
    reset(directory);

    when(idTokenPayload.getEmail()).thenReturn(BLACKLISTED_SA_EMAIL);
    var response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));

    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(blacklistedMessage(BLACKLISTED_SA_EMAIL)));

    var expectedResult = ServiceAccountUsageAuthorizer.ServiceAccountUsageAuthorizationResult.builder()
        .authorized(false)
        .blacklisted(true)
        .message(blacklistedMessage(BLACKLISTED_SA_EMAIL))
        .build();
    var result = sut.checkServiceAccountUsageAuthorization(SERVICE_ACCOUNT, BLACKLISTED_SA_EMAIL);
    assertThat(result, is(expectedResult));

    verifyZeroInteractions(iam);
    verifyZeroInteractions(crm);
    verifyZeroInteractions(directory);
  }


  @Test
  public void shouldDenyAccessIfNoPolicyBinding() throws IOException {
    when((Object) getIamPolicy.execute()).thenReturn(
        new com.google.api.services.cloudresourcemanager.model.Policy());
    when((Object) iam.projects().serviceAccounts().getIamPolicy(any()).execute()).thenReturn(
        new com.google.api.services.iam.v1.model.Policy());

    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(SERVICE_ACCOUNT)));

    verify(iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)).execute();
    verify(getIamPolicy).execute();

    verify(directory.members(), never()).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    verify(directory.members(), never()).hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);

    verifyCheckFails();
  }

  private void verifyCheckFails() {
    var expectedResult = ServiceAccountUsageAuthorizer.ServiceAccountUsageAuthorizationResult.builder()
        .authorized(false)
        .blacklisted(false)
        .message(deniedMessage(SERVICE_ACCOUNT))
        .serviceAccountProjectId(SERVICE_ACCOUNT_PROJECT)
        .build();
    var result = sut.checkServiceAccountUsageAuthorization(SERVICE_ACCOUNT, PRINCIPAL_EMAIL);
    assertThat(result, is(expectedResult));
  }

  @Test
  public void shouldDenyAccessIfPrincipalDoesNotHaveUserRole() throws IOException {
    final Response<?> response = assertThrowsResponseException(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken));
    verify(authorizationPolicy).shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken);
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(SERVICE_ACCOUNT)));

    verify(iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + SERVICE_ACCOUNT)).execute();
    verify(getIamPolicy).execute();
    verify(directory.members()).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    verify(directory.members()).hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);

    verifyCheckFails();
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
    when((Object) getIamPolicy.execute()).thenThrow(cause);
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
  public void shouldAuthorizeIfPrincipalIsAdminUserDirectly(String serviceAccount) {
    when(idTokenPayload.getEmail()).thenReturn(ADMIN_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalIsAdminServiceAccountDirectly(String serviceAccount) {
    when(idTokenPayload.getEmail()).thenReturn(ADMIN_AGENT_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalIsAdminViaGroup(String serviceAccount) throws IOException {
    doReturn(isMember).when(members).hasMember(STYX_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalIsAdminViaGroupEvenCheckRoleFails(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(418);
    var errorRequest = mock(Directory.Members.HasMember.class);
    doThrow(cause).when(errorRequest).execute();
    doReturn(errorRequest).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    doReturn(isMember).when(members).hasMember(STYX_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }


  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldAuthorizeIfPrincipalHasUserRoleOnProjectViaGroup(String serviceAccount) throws IOException {
    doReturn(isMember).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldDenyAccessIfPrincipalHasUserRoleOnProjectViaNonexistGroup(String serviceAccount)
      throws IOException {
    var notFoundRequest = mock(Directory.Members.HasMember.class);
    doThrow(googleJsonResponseException(404)).when(notFoundRequest).execute();
    doReturn(notFoundRequest).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    final Response<?> response =
        assertThrowsResponseException(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(serviceAccount)));

    verifyCheckFails();
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldDenyAccessIfGroupMemberCheckReturnClientError(String serviceAccount)
      throws IOException {
    final Throwable cause = googleJsonResponseException(400);
    var errorRequest = mock(Directory.Members.HasMember.class);
    doThrow(cause).when(errorRequest).execute();
    doReturn(errorRequest).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    final Response<?> response =
        assertThrowsResponseException(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
    assertThat(response.status().code(), is(FORBIDDEN.code()));
    assertThat(response.status().reasonPhrase(), is(deniedMessage(serviceAccount)));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfGroupMemberCheckFails(String serviceAccount)
      throws IOException {
    final Throwable cause = googleJsonResponseException(418);
    var errorRequest = mock(Directory.Members.HasMember.class);
    doThrow(cause).when(errorRequest).execute();
    doReturn(errorRequest).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfGroupMemberCheckFailsForNonGoogleJsonResponseException(String serviceAccount)
      throws IOException {
    final Throwable cause = new IllegalStateException();
    var errorRequest = mock(Directory.Members.HasMember.class);
    doThrow(cause).when(errorRequest).execute();
    doReturn(errorRequest).when(members).hasMember(PROJECT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
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
    doReturn(isMember).when(members).hasMember(SERVICE_ACCOUNT_ADMINS_GROUP_EMAIL, PRINCIPAL_EMAIL);
    assertCachedSuccess(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken));
  }

  @Parameters({SERVICE_ACCOUNT, MANAGED_SERVICE_ACCOUNT})
  @Test
  public void shouldFailIfProjectDoesNotExist(String serviceAccount) throws IOException {
    final Throwable cause = googleJsonResponseException(404);
    when((Object) getIamPolicy.execute()).thenThrow(cause);
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
    when((Object) getIamPolicy.execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(getIamPolicy, atLeast(RETRY_ATTEMPTS)).execute();
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
    when((Object) getIamPolicy.execute()).thenThrow(cause);
    assertThat(Throwables.getRootCause(Try.run(() ->
        sut.authorizeServiceAccountUsage(WORKFLOW_ID, serviceAccount, idToken)).getCause()), is(cause));
    verify(getIamPolicy, atLeast(RETRY_ATTEMPTS)).execute();
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
    final ServiceAccountUsageAuthorizer sut = ServiceAccountUsageAuthorizer.create(
        Closer.create(), SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy, credential, GSUITE_USER_EMAIL,
        "foo", MESSAGE, ADMINISTRATORS, BLACKLIST);
    assertThat(sut, is(notNullValue()));
  }

  @Test
  public void createShouldFailIfCredentialIsNotAServiceAccount() {
    credential = GoogleCredentials.newBuilder().build();
    try {
      ServiceAccountUsageAuthorizer.create(Closer.create(), SERVICE_ACCOUNT_USER_ROLE, authorizationPolicy,
          credential, GSUITE_USER_EMAIL, "foo", MESSAGE, ADMINISTRATORS, BLACKLIST);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("Credential is not a service account"));
    }
  }

  @Test
  public void testNop() {
    final ServiceAccountUsageAuthorizer sut = ServiceAccountUsageAuthorizer.nop();
    assertThat(Try.run(() -> sut.authorizeServiceAccountUsage(WORKFLOW_ID, SERVICE_ACCOUNT, idToken)).isSuccess(),
        is(true));
  }

  @Test
  public void allAuthorizationPolicyShouldEnforce() {
    final AuthorizationPolicy policy = new AllAuthorizationPolicy();
    assertThat(policy.shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken), is(true));
  }

  @Test
  public void whitelistAuthorizationPolicyShouldEnforceWhitelist() {
    final AuthorizationPolicy policy = new WhitelistAuthorizationPolicy(List.of(WORKFLOW_ID));
    assertThat(policy.shouldEnforceAuthorization(WORKFLOW_ID, SERVICE_ACCOUNT, idToken), is(true));
    assertThat(policy.shouldEnforceAuthorization(WorkflowId.create("another", "workflow"), SERVICE_ACCOUNT, idToken),
        is(false));
  }

  @Test
  public void shouldCreateConfiguredServiceAccountUsageAuthorizer() {
    final Config config = ConfigFactory.parseMap(Map.of(
        AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG, SERVICE_ACCOUNT_USER_ROLE,
        AUTHORIZATION_GSUITE_USER_CONFIG, GSUITE_USER_EMAIL,
        AUTHORIZATION_MESSAGE_CONFIG, MESSAGE,
        AUTHORIZATION_ADMINISTRATORS_CONFIG, ADMINISTRATORS,
        AUTHORIZATION_BLACKLIST_CONFIG, BLACKLIST));
    when(environment.config()).thenReturn(config);
    when(environment.closer()).thenReturn(Closer.create());
    final ServiceAccountUsageAuthorizer authorizer = ServiceAccountUsageAuthorizer.create(environment, "foo", credential);
    assertThat(authorizer, is(instanceOf(ServiceAccountUsageAuthorizer.Impl.class)));
  }

  @Test
  public void shouldCreateNopServiceAccountUsageAuthorizer() {
    final Config config = ConfigFactory.parseMap(Map.of());
    when(environment.config()).thenReturn(config);
    final ServiceAccountUsageAuthorizer authorizer = ServiceAccountUsageAuthorizer.create(environment,
        "foo", credential);
    assertThat(authorizer, is(ServiceAccountUsageAuthorizer.nop()));
  }

  @Test
  public void shouldCreateAllAuthorizationPolicy() {
    final Config config = ConfigFactory.parseMap(Map.of(AUTHORIZATION_REQUIRE_ALL_CONFIG, "true"));
    final AuthorizationPolicy policy = AuthorizationPolicy.fromConfig(config);
    assertThat(policy, is(instanceOf(AllAuthorizationPolicy.class)));
  }

  @Test
  public void shouldCreateWhitelistAuthorizationPolicy() {
    final Config config = ConfigFactory.parseMap(Map.of(AUTHORIZATION_REQUIRE_WORKFLOWS,
        List.of("foo#bar", "baz#quux")));
    final AuthorizationPolicy policy = AuthorizationPolicy.fromConfig(config);
    assertThat(policy, is(instanceOf(WhitelistAuthorizationPolicy.class)));
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
    return new GoogleJsonResponseException(new HttpResponseException.Builder(code, "", new HttpHeaders()),
        new GoogleJsonError());
  }

  private static String deniedMessage(String serviceAccount) {
    return "The principal " + PRINCIPAL_EMAIL + " must have the role " +
           SERVICE_ACCOUNT_USER_ROLE + " in the project " + SERVICE_ACCOUNT_PROJECT + " or on the service account " +
           serviceAccount + ", either through a group membership or directly. " + MESSAGE;
  }

  private static String blacklistedMessage(String email) {
    return "The principal " + email + " is blacklisted. Please use another one. " + MESSAGE;
  }
}
