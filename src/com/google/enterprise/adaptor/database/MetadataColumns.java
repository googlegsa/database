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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Stores mapping of db columns to metadata keys to send.
 * This class is thread-safe.
 */
class MetadataColumns extends AbstractMap<String, String> {
  private static final Logger log
      = Logger.getLogger(MetadataColumns.class.getName());

  private final Map<String, String> columnNameToMetadataKey;

  /** Constructs an identity Map from the ResultSetMetaData. */
  MetadataColumns(ResultSetMetaData rsMetaData) throws SQLException {
    Map<String, String> tmp = new TreeMap<>();
    int numberOfColumns = rsMetaData.getColumnCount();
    for (int i = 1; i <= numberOfColumns; i++) {
      String columnName = rsMetaData.getColumnLabel(i);
      tmp.put(columnName, columnName);
    }
    columnNameToMetadataKey = Collections.unmodifiableMap(tmp);
  }

  /**
   * Constructs a Map based upon a configuration string of the form:
   * dbColumnName[:metadataLabel][, ...]
   */
  MetadataColumns(String configDef) {
    if ("".equals(configDef.trim())) {
      columnNameToMetadataKey = Collections.emptyMap();
      return;
    }
    
    Map<String, String> tmp = new TreeMap<String, String>();
    String elements[] = configDef.split(",", 0); // drop trailing empties
    for (String e : elements) {
      log.fine("element: `" + e + "'");
      e = e.trim(); 
      String def[] = e.split(":", 2);
      if (def.length == 1) {
        String columnName = def[0].trim();
        if (!columnName.isEmpty()) {
          tmp.put(columnName, columnName);
        }
      } else if (def.length == 2) {
          tmp.put(def[0].trim(), def[1].trim());
      }
    }
    columnNameToMetadataKey = Collections.unmodifiableMap(tmp);
  }

  @Override
  public Set<Map.Entry<String, String>> entrySet() {
    return columnNameToMetadataKey.entrySet();
  }
}
