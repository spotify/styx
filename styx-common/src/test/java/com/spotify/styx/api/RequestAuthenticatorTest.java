package com.spotify.styx.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Request;
import com.spotify.apollo.Status;
import com.spotify.styx.api.Middlewares.AuthContext;
import java.util.Optional;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestAuthenticatorTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private Authenticator authenticator;
  @Mock private GoogleIdToken idToken;

  private RequestAuthenticator sut;

  @Before
  public void setUp() throws Exception {
    sut = new RequestAuthenticator(authenticator);
  }

  @Test
  public void testAuthorizationHeaderMissing() {
    final Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("hello"));

    final AuthContext authContext = sut.authenticate(request);
    assertThat(authContext.user(), is(Optional.empty()));
  }

  @Test
  public void testAuthorizationMissingBearer() {

    final Request request = Request.forUri("/", "PUT")
        .withHeader(HttpHeaders.AUTHORIZATION, "broken")
        .withPayload(ByteString.encodeUtf8("hello"));

    try {
      sut.authenticate(request);
      fail();
    } catch (ResponseException e) {
      assertThat(e.getResponse().status(), is(
          Status.BAD_REQUEST.withReasonPhrase("Authorization token must be of type Bearer")));
    }
  }

  @Test
  public void testInvalidToken() {

    when(authenticator.authenticate(any())).thenThrow(IllegalArgumentException.class);

    final Request request = Request.forUri("/", "PUT")
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer broken")
        .withPayload(ByteString.encodeUtf8("hello"));

    try {
      sut.authenticate(request);
      fail();
    } catch (ResponseException e) {
      assertThat(e.getResponse().status(), is(
          Status.BAD_REQUEST.withReasonPhrase("Failed to parse Authorization token")));
    }
  }

  @Test
  public void testAuthenticationFailure() {
    when(authenticator.authenticate(any())).thenReturn(null);

    final Request request = Request.forUri("/", "PUT")
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer token")
        .withPayload(ByteString.encodeUtf8("hello"));

    try {
      sut.authenticate(request);
      fail();
    } catch (ResponseException e) {
      assertThat(e.getResponse().status(), is(
          Status.UNAUTHORIZED
              .withReasonPhrase("Authorization token is invalid")));
    }
  }

  @Test
  public void testSuccessfulAuthorization() {
    when(authenticator.authenticate(any())).thenReturn(idToken);

    final Request request = Request.forUri("/", "PUT")
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer token")
        .withPayload(ByteString.encodeUtf8("hello"));

    final AuthContext authContext = sut.authenticate(request);
    assertThat(authContext.user(), is(Optional.of(idToken)));
  }
}