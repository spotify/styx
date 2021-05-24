package com.spotify.styx;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.hash.Hashing;

public class GetGKESecret {
  private GetGKESecret() {}

  public static void main(String[] args) {
    var serviceAccount = "tpm-insights-pipeline@tpm-insights.iam.gserviceaccount.com";
    var suffix = Hashing.sha256().hashString(serviceAccount, UTF_8);
    System.out.println(suffix);
    // Then do:
    // kubectl get secrets | grep <suffix>
  }
}
