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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link ValidatedUri}. */
public class ValidatedUriTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testNullUrl() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri(null);
  }

  @Test
  public void testEmptyUrl() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("");
  }

  @Test
  public void testNoProtocol() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("//foo/bar");
  }

  @Test
  public void testUnknownProtocol() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("unknown://foo/bar");
  }

  @Test
  public void testBadProtocol() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("https//foo/bar");
  }

  @Test
  public void testNoHost() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("http://");
  }

  @Test
  public void testRelativeUri() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("foo/bar");
  }

  @Test
  public void testMessageFormatRemnants() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("http://message_format/foo/{0}");
  }

  @Test
  public void testMessageFormatRemnantsInQuery() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("http://message_format/foo?q={0}");
  }

  @Test
  public void testNakedIPv6Address() throws Exception {
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("http://::1/foo/bar");
  }

  @Test
  public void testBracketedIPv6Address() throws Exception {
    assertEquals(new URI("http://[::1]/foo/bar"),
        new ValidatedUri("http://[::1]/foo/bar").getUri());
  }

  @Test
  public void testRootPath() throws Exception {
    assertEquals(new URI("http://foo:80/"),
        new ValidatedUri("http://foo:80/").getUri());
  }

  @Test
  public void testUnderscoreInHostName() throws Exception {
    // new URI("http://foo_bar/baz") parses incorrectly, but does not throw a
    // URISyntaxException. The host part, however, is discarded. So our check
    // for missing host should catch this.
    thrown.expect(URISyntaxException.class);
    new ValidatedUri("http://foo_bar/baz").getUri();
  }

  @Test
  public void testGetUri() throws Exception {
    assertEquals(new URI("http://example.com/foo/bar"),
        new ValidatedUri("http://example.com/foo/bar").getUri());
  }
}
