package com.spotify.styx;

import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.CreateServiceAccountKeyRequest;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import java.io.IOException;

public class ServiceAccountKeyManager {

  private final Iam iam;

  public ServiceAccountKeyManager(Iam iam) {
    this.iam = iam;
  }

  public ServiceAccountKey createJsonKey(String project, String serviceAccount) throws IOException {
    return createKey(project, serviceAccount, new CreateServiceAccountKeyRequest()
        .setPrivateKeyType("TYPE_GOOGLE_CREDENTIALS_FILE"));
  }

  public ServiceAccountKey createP12Key(String project, String serviceAccount) throws IOException {
    return createKey(project, serviceAccount, new CreateServiceAccountKeyRequest()
        .setPrivateKeyType("TYPE_PKCS12_FILE"));
  }

  private ServiceAccountKey createKey(String project, String serviceAccount,
      CreateServiceAccountKeyRequest request)
      throws IOException {
    return iam.projects().serviceAccounts().keys()
        .create(serviceAccountName(project, serviceAccount), request)
        .execute();
  }

  static String serviceAccountName(String project, String serviceAccount) {
    return "projects/" + project + "/serviceAccounts/" + serviceAccount;
  }
}
