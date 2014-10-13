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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/** Organizes information about database table's primary key. */
class PrimaryKey {
  private static final Logger log
      = Logger.getLogger(PrimaryKey.class.getName());

  static enum ColumnType {
    INT,
    STRING
  }

  private List<String> names = new ArrayList<String>();
  private List<ColumnType> types = new ArrayList<ColumnType>(); 

  PrimaryKey(String configDef) {
    if ("".equals(configDef.trim())) {
      throw new InvalidConfigurationException("empty");
    }
    String elements[] = configDef.split(",", -1);
    for (String e : elements) {
      log.fine("element: " + e);
      String def[] = e.split(":", -1);
      if (2 != def.length) {
        throw new InvalidConfigurationException("bad def: " + e);
      }
      String name = def[0];
      ColumnType type;
      if ("int".equals(def[1].toLowerCase())) {
        type = ColumnType.INT;
      } else if ("string".equals(def[1].toLowerCase())) {
        type = ColumnType.STRING;
      } else {
        throw new InvalidConfigurationException("bad type: " + def[1]);
      } 
      names.add(name);
      types.add(type);
    }
  }

  /** Number of columns that make up the primary key. */
  int numElements() {
    return names.size(); 
  }

  /** Name of particular column in primary key. */
  String name(int i) {
    return names.get(i);
  }

  /** Type of particular column in primary key. */
  ColumnType type(int i) {
    return types.get(i);
  }

  public String toString() {
    return "PrimaryKey(" + names + "," + types + ")";
  }

  String makeUniqueId(ResultSet rs) throws SQLException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < names.size(); i++) {
      String part;
      switch (types.get(i)) {
        case INT:
          part = "" + rs.getInt(names.get(i));
          break;
        case STRING:
          part = rs.getString(names.get(i));
          break;
        default:
          throw new AssertionError("invalid type: " + types.get(i)); 
      } 
      sb.append("/").append(part); // TODO: deal with data with slash
    }
    return sb.toString().substring(1);
  }

  void setStatementValues(PreparedStatement st, String uniqueId)
      throws SQLException {
    String parts[] = uniqueId.split("/", -1); // TODO: deal with slash
    if (parts.length != names.size()) {
      throw new IllegalStateException("id does not match key: " + uniqueId);
    }
    for (int i = 0; i < names.size(); i++) {
      switch (types.get(i)) {
        case INT:
          st.setInt(i + 1, Integer.parseInt(parts[i]));
          break;
        case STRING:
          st.setString(i + 1, parts[i]);
          break;
        default:
          throw new IllegalStateException("invalid type: " + types.get(i)); 
      }
    }   
  }
}
