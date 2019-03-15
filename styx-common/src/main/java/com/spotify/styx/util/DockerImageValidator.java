/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.util;

import static java.lang.String.format;

import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adopted from https://github.com/spotify/helios/blob/master/helios-client/src/main/java/com/spotify/helios/common/JobValidator.java
 */
public class DockerImageValidator {

  private static final Pattern DOMAIN_PATTERN =
      Pattern.compile("^(?:(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9]))"
          + "(\\.(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])))*)\\.?$");

  private static final Pattern IPV4_PATTERN =
      Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

  private static final Pattern NAME_COMPONENT_PATTERN = Pattern.compile("^([a-z0-9._-]+)$");
  private static final int REPO_NAME_MAX_LENGTH = 255;

  // https://github.com/docker/distribution/blob/95daa793b83a21656fe6c13e6d5cf1c3999108c7/reference/regexp.go#L37
  private static final Pattern TAG_PATTERN = Pattern.compile("[\\w][\\w.-]{0,127}");

  // https://github.com/docker/distribution/blob/95daa793b83a21656fe6c13e6d5cf1c3999108c7/reference/regexp.go#L44
  private static final Pattern DIGEST_PATTERN = Pattern.compile(
      "(?<algorithm>[A-Za-z][A-Za-z0-9]*(?:[-_+.][A-Za-z][A-Za-z0-9]*)*)[:](?<identifier>[\\p{XDigit}]{32,})");

  // https://github.com/docker/distribution/blob/95daa793b83a21656fe6c13e6d5cf1c3999108c7/reference/regexp.go#L75
  private static final Pattern SHA256_IDENTIFIER_PATTERN = Pattern.compile("[a-f0-9]{64}");

  private static final Pattern DIGIT_PERIOD = Pattern.compile("^[0-9.]+$");

  public Collection<String> validateImageReference(final String imageRef) {
    final Collection<String> errors = new HashSet<>();
    validateImageReference(imageRef, errors);
    return errors;
  }

  @SuppressWarnings("ConstantConditions")
  private boolean validateImageReference(final String imageRef, final Collection<String> errors) {
    boolean valid = true;

    final String repo;
    final String tag;
    final String digest;

    final int lastAtSign = imageRef.lastIndexOf('@');
    final int tagColon;
    final int tagEnd;

    // Parse digest
    if (lastAtSign != -1) {
      digest = imageRef.substring(lastAtSign + 1);
      tagEnd = lastAtSign;
      tagColon = imageRef.lastIndexOf(':', tagEnd);
      valid &= validateDigest(digest, errors);
    } else {
      tagColon = imageRef.lastIndexOf(':');
      tagEnd = imageRef.length();
    }

    // Parse tag
    if (tagColon != -1 && !(tag = imageRef.substring(tagColon + 1, tagEnd)).contains("/")) {
      repo = imageRef.substring(0, tagColon);
      valid &= validateTag(tag, errors);
    } else {
      repo = imageRef.substring(0, tagEnd);
    }

    final String invalidRepoName = "Invalid repository name (ex: \"registry.domain.tld/myrepos\")";

    if (repo.contains("://")) {
      // It cannot contain a scheme!
      errors.add(invalidRepoName);
      return false;
    }

    final String[] nameParts = repo.split("/", 2);
    if (!nameParts[0].contains(".")
        && !nameParts[0].contains(":")
        && !nameParts[0].equals("localhost")) {
      // This is a Docker Index repos (ex: samalba/hipache or ubuntu)
      return validateRepositoryName(imageRef, repo, errors);
    }

    if (nameParts.length < 2) {
      // There is a dot in repos name (and no registry address)
      // Is it a Registry address without repos name?
      errors.add(invalidRepoName);
      return false;
    }

    final String endpoint = nameParts[0];
    final String reposName = nameParts[1];
    valid &= validateEndpoint(endpoint, errors);
    valid &= validateRepositoryName(imageRef, reposName, errors);
    return valid;
  }

  private boolean validateTag(final String tag, final Collection<String> errors) {
    if (tag.isEmpty()) {
      errors.add("Tag cannot be empty");
      return false;
    }
    if (!TAG_PATTERN.matcher(tag).matches()) {
      errors.add(format("Illegal tag: \"%s\", must match %s", tag, TAG_PATTERN));
      return false;
    }
    return true;
  }

  private boolean validateDigest(final String digest, final Collection<String> errors) {
    if (digest.isEmpty()) {
      errors.add("Digest cannot be empty");
      return false;
    }

    final Matcher matcher = DIGEST_PATTERN.matcher(digest);
    if (!matcher.matches()) {
      errors.add(format("Illegal digest: \"%s\"", digest));
      return false;
    }

    final String algorithm = matcher.group("algorithm");
    final String identifier = matcher.group("identifier");

    if ("sha256".equals(algorithm)) {
      if (!SHA256_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
        errors.add(format("Illegal digest: \"%s\"", digest));
        return false;
      }
    }

    return true;
  }

  private boolean validateEndpoint(final String endpoint, final Collection<String> errors) {
    final String[] parts = endpoint.split(":", 2);
    if (!validateAddress(parts[0], errors)) {
      return false;
    }
    if (parts.length > 1) {
      final int port;
      try {
        port = Integer.valueOf(parts[1]);
      } catch (NumberFormatException e) {
        errors.add(format("Invalid port in endpoint: \"%s\"", endpoint));
        return false;
      }
      if (port < 0 || port > 65535) {
        errors.add(format("Invalid port in endpoint: \"%s\"", endpoint));
        return false;
      }
    }
    return true;
  }

  private boolean validateAddress(final String address, final Collection<String> errors) {
    if (IPV4_PATTERN.matcher(address).matches()) {
      return true;
    } else if (!DOMAIN_PATTERN.matcher(address).matches() || DIGIT_PERIOD.matcher(address).find()) {
      errors.add(format("Invalid domain name: \"%s\"", address));
      return false;
    }
    return true;
  }

  private boolean validateRepositoryName(
      final String imageName,
      final String repositoryName,
      final Collection<String> errors) {

    /*
    From https://github.com/docker/docker/commit/ea98cf74aad3c2633268d5a0b8a2f80b331ddc0b:
    The image name which is made up of slash-separated name components, ....
    Name components may contain lowercase characters, digits and separators. A separator is defined
    as a period, one or two underscores, or one or more dashes. A name component may not start or
    end with a separator.
     */
    final String[] nameParts = repositoryName.split("/");
    for (String name : nameParts) {
      if (!NAME_COMPONENT_PATTERN.matcher(name).matches()) {
        errors.add(
            format("Invalid image name (%s), only %s is allowed for each slash-separated "
                    + "name component (failed on \"%s\")",
                imageName, NAME_COMPONENT_PATTERN, name)
        );
        return false;
      }
    }

    if (repositoryName.length() > REPO_NAME_MAX_LENGTH) {
      errors.add(
          format("Invalid image name (%s), repository name cannot be larger than %d characters",
              imageName, REPO_NAME_MAX_LENGTH)
      );
      return false;
    }
    return true;
  }
}
