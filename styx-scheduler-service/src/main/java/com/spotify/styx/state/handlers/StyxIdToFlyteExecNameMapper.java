/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.state.handlers;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.abs;

import com.google.common.io.BaseEncoding;
import java.util.Arrays;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Map Styx id to Flyte exec names.
 *
 * Flyte exec names should be 20 characters long and it have conform to DNS-1123 subdomain,
 * however for some reason it must start with a lowercase alphabetic character.
 */
class StyxIdToFlyteExecNameMapper implements Function<String, String> {

  private static final String STYX_ID_PREFIX = "styx-run-";
  private static final String UUID_REGEX = "[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}";
  private static final Pattern STYX_RUN_ID_REGEX = Pattern.compile("^" + STYX_ID_PREFIX + UUID_REGEX + "$");

  private static final int FLYTE_EXEC_NAME_SIZE = 20;
  private static final BaseEncoding BASE16_ENCODER = BaseEncoding.base16().lowerCase();
  private static final BaseEncoding BASE32_ENCODER = BaseEncoding.base32().omitPadding().lowerCase();
  private static final String ALPHAS = "abcdefghijklmnopqrstuvwxyz";

  @Override
  public String apply(String styxRunId) {
    checkArgument(
        STYX_RUN_ID_REGEX.matcher(styxRunId).matches(),
        "Not valid styx run id: [%s]",
        styxRunId);
    // hex encodes only 4 bits per character
    var hexEncodedUuid = styxRunId.substring(STYX_ID_PREFIX.length()).replace("-", "");
    var uuidBytes = BASE16_ENCODER.decode(hexEncodedUuid);

    // base32 encodes 5 bits per character
    var base32EncodedUuid = BASE32_ENCODER.encode(uuidBytes);

    // I am not happy with this solution yet but it is good for now.
    // We are encoding 19 * 5 bits of entropy plus whatever we get from the start character.
    // According to https://en.wikipedia.org/wiki/Birthday_problem#Probability_table,
    // we should expect a collision with a probability around 1*10^-12 once we have created 4.0×10^8 names.
    var start = ALPHAS.charAt(abs(Arrays.hashCode(uuidBytes) % ALPHAS.length()));
    return (start + base32EncodedUuid).substring(0, FLYTE_EXEC_NAME_SIZE);
  }
}
