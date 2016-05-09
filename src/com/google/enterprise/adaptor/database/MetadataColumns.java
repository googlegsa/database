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

import com.google.enterprise.adaptor.InvalidConfigurationException;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Stores mapping of db columns to metadata keys to send.
 * This class is thread-safe.
 */
class MetadataColumns {
  private static final Logger log
      = Logger.getLogger(MetadataColumns.class.getName());

  private final Map<String, String> columnNameToMetadataKey;

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
      if (2 != def.length) {
        String errmsg = "expected two parts separated by colon: `" + e + "'";
        throw new InvalidConfigurationException(errmsg);
      }
      String columnName = def[0].trim();
      String metadataKey = def[1].trim();
      tmp.put(columnName, metadataKey);
    }
    columnNameToMetadataKey = Collections.unmodifiableMap(tmp);
  }

  public String getMetadataName(String columnName) {
    return columnNameToMetadataKey.get(columnName);
  }

  @Override
  public String toString() {
    return "MetadataColumns(" + columnNameToMetadataKey + ")";
  }

  @Override
  public boolean equals(Object other) {
    boolean same = false;
    if (other instanceof MetadataColumns.AllColumns) {
      return same;
    }
    else if (other instanceof MetadataColumns) {
      MetadataColumns mc = (MetadataColumns) other;
      same = columnNameToMetadataKey.equals(mc.columnNameToMetadataKey);
    }
    return same;
  }

  @Override
  public int hashCode() {
    return columnNameToMetadataKey.hashCode();
  }

  /**
   * Passes through all columns from the resultSet as Metadata.
   */
  static class AllColumns extends MetadataColumns {
    public AllColumns() {
      super("");
    }

    @Override
    public String getMetadataName(String columnName) {
      return columnName;
    }

    @Override
    public String toString() {
      return "MetadataColumns.AllColumns()";
    }

    @Override
    public boolean equals(Object other) {
      return (other instanceof MetadataColumns.AllColumns);
    }
  };
}
