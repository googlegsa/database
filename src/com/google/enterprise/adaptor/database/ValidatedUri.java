// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.adaptor.database;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Validates URIs by syntax checking and validating the host is reachable.
 */
class ValidatedUri {

  /** The validated URI. */
  private final URI uri;

  /**
   * Attempts to validate the given URI syntax with some more stringent checks
   * than new URI. In this case, we're mostly trying to catch typos and obvious
   * configuration issues.
   *
   * @param uriString the URI to test
   * @throws URISyntaxException if the URL syntax is invalid
   */
  public ValidatedUri(String uriString) throws URISyntaxException {
    if (Strings.isNullOrEmpty(uriString)) {
      throw new URISyntaxException("" + uriString, "null or empty URI");
    }
    uriString = fixupUri(uriString);
    try {
      // Basic syntax checking, with more understandable error messages.
      // Also ensures the URI is a URL, not a URN, and is absolute.
      new URL(uriString);
      // Advanced syntax checking, with more cryptic error messages.
      uri = new URI(uriString);
      uri.toURL();
    } catch (MalformedURLException e) {
      int index = e.getMessage().lastIndexOf(": " + uriString);
      String reason = (index < 0)
          ? e.getMessage() : e.getMessage().substring(0, index);
      throw new URISyntaxException(uriString, reason);
    }

    if (Strings.isNullOrEmpty(uri.getHost())) {
      throw new URISyntaxException(uriString, "no host");
    }
  }

  // Fix up common URI syntax errors, backslashes, spaces, unescaped chars etc.
  private static String fixupUri(String uriString) {
    String[] parts = uriString.split("\\?", 2);
    StringBuilder builder = new StringBuilder();
    percentEncode(builder, parts[0].replace('\\', '/'));
    if (parts.length == 2) {
      builder.append('?');
      percentEncode(builder, parts[1]);
    }
    return builder.toString();
  }

  private static final String PERCENT =
      "%!%#$%&'()*+,-./0123456789:;%=%?"
      + "@ABCDEFGHIJKLMNOPQRSTUVWXYZ[%]%_"
      + "%abcdefghijklmnopqrstuvwxyz{%}~";

  private static final String HEX = "0123456789ABCDEF";

  /**
   * Percent-encode {@code text} as described in
   * <a href="http://tools.ietf.org/html/rfc3986#section-2">RFC 3986</a> and
   * using UTF-8. This is the most common form of percent encoding.
   * The characters A-Z, a-z, 0-9, '-', '_', '.', and '~' are left as-is per
   * RFC 3986. All RFC 3986 {@code reserved} characters are left as-is and
   * assumed they are used correctly. Percent '%' characters are left as-is to
   * preserve any existing percent encoding. The characters '{' and '}' are
   * left as-is to catch MessageFormat errors. The rest are percent encoded.
   *
   * This was adapted from v3 Connector Manager's ServletUtil.
   *
   * @param sb StringBuilder in which to encode the text
   * @param text some plain text
   */
  private static void percentEncode(StringBuilder sb, String text) {
    byte[] bytes = text.getBytes(Charsets.UTF_8);
    for (byte b : bytes) {
      if (b >= ' ' && b <= '~' && PERCENT.charAt(b - ' ') != '%') {
        sb.append((char) b);
      } else if ((char) b == '%') {
        sb.append((char) b);
      } else {
        sb.append('%');
        sb.append(HEX.charAt((b >> 4) & 0x0F));
        sb.append(HEX.charAt(b & 0x0F));
      }
    }
  }

  /**
   * Returns the validated URI.
   */
  public URI getUri() {
    return uri;
  }
}
