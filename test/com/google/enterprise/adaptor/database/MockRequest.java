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

import com.google.enterprise.adaptor.DocId;
import com.google.enterprise.adaptor.Request;

import java.util.Date;

/** A trivial implemenation of {@link Request} */
// Copied from the Filesystem Adaptor
class MockRequest implements Request {
  private final DocId docid;
  private final Date lastAccess;

  MockRequest(DocId docid) {
    this(docid, null);
  }

  MockRequest(DocId docid, Date lastAccess) {
    this.docid = docid;
    this.lastAccess = lastAccess;
  }

  @Override
  public boolean hasChangedSinceLastAccess(Date lastModified) {
    return (lastAccess == null || lastModified == null) ? true
        : lastModified.after(lastAccess);
  }

  @Override
  public Date getLastAccessTime() {
    return lastAccess;
  }

  @Override
  public DocId getDocId() {
    return docid;
  }

  @Override
  public boolean canRespondWithNoContent(Date lastModified) {
    return !hasChangedSinceLastAccess(lastModified);
  }
}
