// Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.enterprise.adaptor.Acl;
import com.google.enterprise.adaptor.Metadata;
import com.google.enterprise.adaptor.Response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link Response} that implements only those items that
 * the adaptor uses.
 */
// Copied from the Filesystem Adaptor, enhanced to support anchors and
// multivalued metadata.
class MockResponse implements Response {
  boolean noContent = false;
  boolean notModified = false;
  boolean notFound = false;
  boolean noIndex = false;
  boolean noFollow = false;
  String contentType;
  Date lastModified;
  URI displayUrl;
  Acl acl;
  Metadata metadata = new Metadata();
  Map<String, Acl> namedResources = new HashMap<String, Acl>();
  Map<String, URI> anchors = new HashMap<String, URI>();
  ByteArrayOutputStream content;

  @Override
  public void respondNoContent() {
    noContent = true;
  }

  @Override
  public void respondNotModified() throws IOException {
    notModified = true;
  }

  @Override
  public void respondNotFound() throws IOException {
    notFound = true;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    content = new ByteArrayOutputStream();
    return content;
  }

  @Override
  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  @Override
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }

  @Override
  public void addMetadata(String key, String value) {
    metadata.add(key, value);
  }

  @Override
  public void setAcl(Acl acl) {
    this.acl = acl;
  }

  @Override
  public void putNamedResource(String fragment, Acl acl) {
    namedResources.put(fragment, acl);
  }

  @Override
  public void setDisplayUrl(URI displayUrl) {
    this.displayUrl = displayUrl;
  }

  @Override
  public void setSecure(boolean secure) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addAnchor(URI uri, String text) {
    anchors.put(text, uri);
  }

  @Override
  public void setNoIndex(boolean noIndex) {
    this.noIndex = noIndex;
  }

  @Override
  public void setNoFollow(boolean noFollow) {
    this.noFollow = noFollow;
    //throw new UnsupportedOperationException();
  }

  @Override
  public void setNoArchive(boolean noArchive) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setCrawlOnce(boolean crawlOnce) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLock(boolean lock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return String.format("{ notFound: %b, noIndex: %b, notModified: %b, "
        + "lastModified: %s, displayUrl: %s, contentType: %s, content: '%s', "
        + "metadata: %s, namedResources: %s, anchors: %s }",
        notFound, noIndex, notModified, lastModified, displayUrl,
        contentType, content, metadata, namedResources, anchors);
  }
}
