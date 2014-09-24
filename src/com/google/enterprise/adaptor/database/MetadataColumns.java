// Copyright 2013 Google Inc. All Rights Reserved.
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.logging.Logger;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stores mapping of db columns to metadata keys to send.
 * This class is thread-safe.
 */
class MetadataColumns {
  private static final Logger log
      = Logger.getLogger(MetadataColumns.class.getName());

  private final Map<String, String> columnNameToMetadataKey;

  MetadataColumns(String configDef) {
    Map<String, String> tmp = new TreeMap<String, String>();
    if ("".equals(configDef.trim())) {
      throw new IllegalArgumentException("empty");
    }
    String elements[] = configDef.split(",", -1);
    for (String e : elements) {
      log.info("element: " + e);
      String def[] = e.split(":", 2);
      if (2 != def.length) {
        String emsg = "expected two parts separated by colon: " + e;
        throw new IllegalArgumentException(emsg);
      }
      String columnName = def[0];
      String metadataKey = def[1];
      tmp.put(columnName, metadataKey);
    }
    columnNameToMetadataKey = Collections.unmodifiableMap(tmp);
  }

  public boolean isMetadataColumnName(String columnName) {
    return columnNameToMetadataKey.containsKey(columnName);
  }

  public String getMetadataName(String columnName) {
    return columnNameToMetadataKey.get(columnName);
  }

  public String toString() {
    return "MetadataColumns" + columnNameToMetadataKey;
  }
}
